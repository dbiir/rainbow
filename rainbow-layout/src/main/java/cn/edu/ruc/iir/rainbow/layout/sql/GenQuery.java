package cn.edu.ruc.iir.rainbow.layout.sql;

import java.io.*;

public class GenQuery
{

	public static void gen(String tableName, String hostName) throws IOException
	{

		BufferedReader jobsReader = new BufferedReader(new FileReader("cord-generator/resources/duped_jobs.txt"));

		BufferedWriter sparkWriter = new BufferedWriter(new FileWriter("cord-generator/resources/sql/spark.sql"));

		BufferedWriter hiveWriter = new BufferedWriter(new FileWriter("cord-generator/resources/sql/hive.sql"));

		BufferedWriter columnWriter = new BufferedWriter(new FileWriter("cord-generator/resources/sql/query.column"));


		String line = null;
		String orderedTableName = "ordered_" + tableName;
		tableName = tableName + "_default";
		int i = 0;

		while ((line = jobsReader.readLine()) != null)
		{
			String tokens[] = line.split("\t");
			String columns = tokens[1].toLowerCase();
			sparkWriter.write("% query " + i++ + "\n");
			sparkWriter.write("val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n");
			sparkWriter.write("import sqlContext.createSchemaRDD\n");
			sparkWriter.write("val parquetFile = sqlContext.parquetFile(\"hdfs://" + hostName + ":9000/" + orderedTableName + "\")\n");
			sparkWriter.write("parquetFile.registerTempTable(\"ordered_parq\")\n");
			sparkWriter.write("val res = sqlContext.sql(\"select " + columns + " from ordered_parq limit 3000\")\n");
			sparkWriter.write("res.count()\n");
			sparkWriter.newLine();
			sparkWriter.write("val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n");
			sparkWriter.write("import sqlContext.createSchemaRDD\n");
			sparkWriter.write("val parquetFile = sqlContext.parquetFile(\"hdfs://" + hostName + ":9000/" + tableName + "\")\n");
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
			columnWriter.write(columns);
			columnWriter.newLine();
		}
		sparkWriter.close();
        columnWriter.close();
        hiveWriter.close();

	}

    public static void main(String[] args) throws IOException
    {
        gen ("orc", "n231");
    }
}
