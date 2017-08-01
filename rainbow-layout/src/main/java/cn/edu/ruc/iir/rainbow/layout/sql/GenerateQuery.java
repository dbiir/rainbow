package cn.edu.ruc.iir.rainbow.layout.sql;

import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.InputFactory;
import cn.edu.ruc.iir.rainbow.layout.builder.ColumnOrderBuilder;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateQuery
{
	private GenerateQuery() {}

	private static List<String> genMergedJobs(String schemaFilePath, String workloadFilePath) throws IOException, ColumnNotFoundException
	{
		List<String> mergedJobs = new ArrayList<>();
		try (BufferedReader originWorkloadReader = InputFactory.Instance().getReader(workloadFilePath))
		{

			List<Query> workload = new ArrayList<>();
			Map<String, Column> columnMap = new HashMap<String, Column>();
			List<Column> columnOrder = ColumnOrderBuilder.build(new File(schemaFilePath));
			Map<Integer, String> columnIdToNameMap = new HashMap<>();
			for (Column column : columnOrder)
			{
				columnIdToNameMap.put(column.getId(), column.getName());
				columnMap.put(column.getName().toLowerCase(), column);
			}

			String line = null;
			int qid = 0;
			while ((line = originWorkloadReader.readLine()) != null)
			{
				String[] tokens = line.split("\t");
				String[] columnNames = tokens[2].split(",");
				Query query = new Query(qid, tokens[0], Double.parseDouble(tokens[1]));
				for (String columnName : columnNames)
				{
					Column column = columnMap.get(columnName.toLowerCase());
					if (column == null)
					{
						throw new ColumnNotFoundException("column " + columnName + " from query " +
								query.getId() + " is not found in the columnOrder.");
					}
					query.addColumnId(column.getId());
					column.addQueryId(qid);
				}

				boolean patternExists = false;
				for (Query query1 : workload)
				{
					boolean patternEquals = true;
					for (int columnId : query.getColumnIds())
					{
						if (!query1.getColumnIds().contains(columnId))
						{
							patternEquals = false;
							break;
						}
					}
					if (patternEquals && query.getColumnIds().size() == query1.getColumnIds().size())
					{
						query1.addWeight(query.getWeight());
						patternExists = true;
						break;
					}
				}
				if (patternExists == false)
				{
					workload.add(query);
					qid++;
				}
			}
			for (Query query : workload)
			{
				StringBuilder builder = new StringBuilder(query.getSid() + "\t");
				List<Integer> columnIds = new ArrayList<>(query.getColumnIds());
				String columnName = columnIdToNameMap.get(columnIds.get(0));
				builder.append(columnName);
				for (int i = 1; i < columnIds.size(); ++i)
				{
					builder.append("," + columnIdToNameMap.get(columnIds.get(i)));
				}
				mergedJobs.add(builder.toString());
			}
		}

		return mergedJobs;
	}

    private static final String dataDir = ConfigFactory.Instance().getProperty("data.dir");

    /**
     * Currently only Parquet data format is supported in generated Spark queries.
     * @param tableName
     * @param orderedTableName
     * @param hostName
     * @param schemaFilePath
     * @param workloadFilePath
     * @param sparkQueryPath
     * @param hiveQueryPath
     * @throws IOException
     * @throws ColumnNotFoundException
     */
	public static void Gen(String tableName, String orderedTableName, String hostName, String schemaFilePath, String workloadFilePath,
    String sparkQueryPath, String hiveQueryPath) throws IOException, ColumnNotFoundException
	{
		List<String> mergedJobs = genMergedJobs(schemaFilePath, workloadFilePath);

		try (BufferedWriter sparkWriter = new BufferedWriter(new FileWriter(sparkQueryPath));
			 BufferedWriter hiveWriter = new BufferedWriter(new FileWriter(hiveQueryPath)))
		{
			int i = 0;

			for (String line : mergedJobs)
			{
				String tokens[] = line.split("\t");
				String columns = tokens[1].toLowerCase();
				sparkWriter.write("% query " + i++ + "\n");
				sparkWriter.write("val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n");
				sparkWriter.write("import sqlContext.createSchemaRDD\n");
				sparkWriter.write("val parquetFile = sqlContext.parquetFile(\"hdfs://" + hostName + ":9000" + dataDir + "/" + orderedTableName + "\")\n");
				sparkWriter.write("parquetFile.registerTempTable(\"ordered_parq\")\n");
				sparkWriter.write("val res = sqlContext.sql(\"select " + columns + " from ordered_parq limit 3000\")\n");
				sparkWriter.write("res.count()\n");
				sparkWriter.newLine();
				sparkWriter.write("val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n");
				sparkWriter.write("import sqlContext.createSchemaRDD\n");
				sparkWriter.write("val parquetFile = sqlContext.parquetFile(\"hdfs://" + hostName + ":9000" + dataDir + "/" + tableName + "\")\n");
				sparkWriter.write("parquetFile.registerTempTable(\"parq\")\n");
				sparkWriter.write("val res = sqlContext.sql(\"select " + columns + " from parq limit 3000\")\n");
				sparkWriter.write("res.count()\n");
				sparkWriter.newLine();
				sparkWriter.newLine();
				hiveWriter.write("select " + columns + " from " + orderedTableName + " limit 10;\n");
				hiveWriter.newLine();
				hiveWriter.write("select " + columns + " from " + tableName + " limit 10;\n");
				hiveWriter.newLine();
				hiveWriter.newLine();
			}
		}
	}
}
