package cn.edu.ruc.iir.rainbow.layout.sql;

import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.common.util.InputFactory;
import cn.edu.ruc.iir.rainbow.common.util.OutputFactory;
import cn.edu.ruc.iir.rainbow.layout.builder.ColumnOrderBuilder;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hank on 17-4-28.
 */
public class GenMergedJobs
{
    public static void main(String[] args) throws IOException, ColumnNotFoundException
    {
        BufferedWriter writer = OutputFactory.Instance().getWriter("cord-generator/resources/merged_jobs.txt");
        BufferedReader originWorkloadReader = InputFactory.Instance().getReader("cord-generator/resources/workload.txt");

        List<Query> workload = new ArrayList<>();
        Map<String, Column> columnMap = new HashMap<String, Column>();
        List<Column> columnOrder = ColumnOrderBuilder.build(new File("cord-generator/resources/schema.txt"));
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
                    if (! query1.getColumnIds().contains(columnId))
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
        originWorkloadReader.close();
        for (Query query : workload)
        {
            writer.write(query.getSid() + "\t");
            List<Integer> columnIds = new ArrayList<>(query.getColumnIds());
            String columnName = columnIdToNameMap.get(columnIds.get(0));
            writer.write(columnName);
            for (int i = 1; i < columnIds.size(); ++i)
            {
                writer.write(","+columnIdToNameMap.get(columnIds.get(i)));
            }
            writer.write("\n");
        }
        writer.close();
    }
}
