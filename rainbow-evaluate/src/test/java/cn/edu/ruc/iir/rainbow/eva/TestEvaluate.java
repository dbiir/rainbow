package cn.edu.ruc.iir.rainbow.eva;

import cn.edu.ruc.iir.rainbow.common.exception.MetadataException;
import cn.edu.ruc.iir.rainbow.common.metadata.ParquetMetadataStat;
import cn.edu.ruc.iir.rainbow.eva.domain.Column;
import cn.edu.ruc.iir.rainbow.eva.metrics.LocalMetrics;
import cn.edu.ruc.iir.rainbow.eva.metrics.StageMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import parquet.hadoop.metadata.ParquetMetadata;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by hank on 2015/2/27.
 */
public class TestEvaluate
{
    public static void main (String[] args)
    {
        if (args[0].equalsIgnoreCase("--help") || args[0].equalsIgnoreCase("help"))
        {
            System.out.println("local ordered_hdfs_path unordered_hdfs_path columns_file log_dir(end with '/') drop_cache");
            System.out.println("spark master_hostname ordered_hdfs_path unordered_hdfs_path columns_file log_dir(end with '/') drop_cache");
            System.exit(0);
        }

        if (args[0].equalsIgnoreCase("local"))
        {
            String orderedPath = args[1];
            String unorderedPath = args[2];
            String columnFilePath = args[3];
            String log_dir = args[4];
            boolean dropCache = Boolean.parseBoolean(args[5]);
            Configuration conf = new Configuration();
            try
            {
                // get metadata
                FileStatus[] orderedStatuses = LocalEvaluator.getFileStatuses(orderedPath, conf);
                FileStatus[] unorderedStatuses = LocalEvaluator.getFileStatuses(unorderedPath, conf);
                ParquetMetadata[] orderedMetadatas = LocalEvaluator.getMetadatas(orderedStatuses, conf);
                ParquetMetadata[] unorderedMetadatas = LocalEvaluator.getMetadatas(unorderedStatuses, conf);

                BufferedReader reader = new BufferedReader(new FileReader(columnFilePath));
                BufferedWriter timeWriter = new BufferedWriter(new FileWriter(log_dir + "local_time"));
                BufferedWriter columnWriter = new BufferedWriter(new FileWriter(log_dir + "columns"));
                String columns = null;
                int i = 0;
                while ((columns = reader.readLine()) != null)
                {
                    // evaluate
                    // clear the caches and buffers
                    if (dropCache)
                    {
                        Runtime.getRuntime().exec("/root/drop_caches.sh");
                    }
                    LocalMetrics orderedMetrics = LocalEvaluator.execute(orderedStatuses, orderedMetadatas, columns.split(","), conf);
                    // clear the caches and buffers
                    if (dropCache)
                    {
                        Runtime.getRuntime().exec("/root/drop_caches.sh");
                    }
                    LocalMetrics unorderedMetrics = LocalEvaluator.execute(unorderedStatuses, unorderedMetadatas, columns.split(","), conf);

                    // log the results
                    timeWriter.write(i + "\t" + orderedMetrics.getTimeMillis() + "\t" + unorderedMetrics.getTimeMillis() + "\n");
                    timeWriter.flush();
                    columnWriter.write("[query " + i + "]\nordered:\n");
                    for (Column column : orderedMetrics.getColumns())
                    {
                        columnWriter.write(column.getIndex() + ", " + column.getName() + "\n");
                    }
                    columnWriter.write("\nunordered:\n");
                    for (Column column : unorderedMetrics.getColumns())
                    {
                        columnWriter.write(column.getIndex() + ", " + column.getName() + "\n");
                    }
                    columnWriter.write("\n\n");
                    columnWriter.flush();
                    ++i;
                }
                timeWriter.close();
                columnWriter.close();
                reader.close();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        else if (args[0].equalsIgnoreCase("spark"))
        {
            String masterHostName = args[1];
            String orderedPath = args[2];
            String unorderedPath = args[3];
            String columnFilePath = args[4];
            String log_dir = args[5];
            boolean dropCache = Boolean.parseBoolean(args[6]);
            try
            {
                // get the column sizes
                ParquetMetadataStat stat = new ParquetMetadataStat(masterHostName, 9000, orderedPath.split("9000")[1]);
                System.out.println(masterHostName);
                int n = stat.getFieldNames().size();
                List<String> names = stat.getFieldNames();
                double[] sizes = stat.getAvgColumnChunkSize();
                Map<String, Double> nameSizeMap = new HashMap<String, Double>();
                for (int j = 0; j < n; ++j)
                {
                    //System.out.println(names.get(j) + ", " + sizes[j]);
                    nameSizeMap.put(names.get(j).toLowerCase(), sizes[j]);
                }
                //System.out.println(n);
                // begin evaluate
                BufferedReader reader = new BufferedReader(new FileReader(columnFilePath));
                BufferedWriter timeWriter = new BufferedWriter(new FileWriter(log_dir + "spark_time"));
                String columns;
                int i = 0;
                while ((columns = reader.readLine()) != null)
                {
                    // get the smallest column as the order by column
                    String orderByColumn = null;
                    double size = Double.MAX_VALUE;

                    for (String name : columns.split(","))
                    {
                        //System.out.println(name);
                        if (name.toLowerCase().equals("market"))
                        {
                            orderByColumn = "market";
                            break;
                        }
                    }

                    if (orderByColumn == null)
                    {
                        for (String name : columns.split(","))
                        {
                            //System.out.println(name);
                            if (nameSizeMap.get(name.toLowerCase()) < size)
                            {
                                size = nameSizeMap.get(name.toLowerCase());
                                orderByColumn = name.toLowerCase();
                            }
                        }
                    }

                    // evaluate
                    // clear the caches and buffers
                    if (dropCache)
                    {
                        Runtime.getRuntime().exec("/root/drop_caches.sh");
                    }
                    StageMetrics orderedMetrics = SparkEvaluator.execute("ordered_" + i, masterHostName, orderedPath, columns, orderByColumn);
                    // clear the caches and buffers
                    if (dropCache)
                    {
                        Runtime.getRuntime().exec("/root/drop_caches.sh");
                    }
                    StageMetrics unorderedMetrics = SparkEvaluator.execute("unordered_" + i, masterHostName, unorderedPath, columns, orderByColumn);

                    // log the results
                    timeWriter.write(i + "\t" + orderedMetrics.getDuration() + "\t" + unorderedMetrics.getDuration() + "\n");
                    timeWriter.flush();

                    ++i;
                }
                timeWriter.close();
                reader.close();

            } catch (IOException e)
            {
                e.printStackTrace();
            } catch (MetadataException e)
            {
                e.printStackTrace();
            }
        }
    }
}
