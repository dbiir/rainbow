package cn.edu.ruc.iir.rainbow.eva.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.ProgressListener;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.exception.MetadataException;
import cn.edu.ruc.iir.rainbow.common.metadata.ParquetMetadataStat;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.eva.LocalEvaluator;
import cn.edu.ruc.iir.rainbow.eva.SparkEvaluator;
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
import java.util.Properties;

/**
 * Created by hank on 17-5-4.
 */
public class CmdWorkloadEvaluation implements Command
{
    private Receiver receiver = null;

    @Override
    public void setReceiver(Receiver receiver)
    {
        this.receiver = receiver;
    }

    /**
     * params should contain the following settings:
     * <ol>
     *   <li>method, LOCAL or SPARK</li>
     *   <li>table.dir, the path of unordered table directory on HDFS</li>
     *   <li>workload.file workload file path</li>
     *   <li>log.dir the local directory used to write evaluation results, must end with '/'</li>
     *   <li>drop.cache, true or false, whether or not drop file cache on each node in the cluster</li>
     *   <li>drop.caches.sh, the file path of drop_caches.sh</li>
     * </ol>
     *
     * this method will pass the following results to receiver:
     * <ol>
     *   <li>log.dir</li>
     *   <li>success, true or false</li>
     * </ol>
     * @param params
     */
    @Override
    public void execute(Properties params)
    {
        Properties results = new Properties(params);
        results.setProperty("success", "false");
        ProgressListener progressListener = percentage -> {
            if (receiver != null)
            {
                receiver.progress(percentage);
            }
        };
        progressListener.setPercentage(0.0);

        String tablePath = params.getProperty("table.dir");
        String workloadFilePath = params.getProperty("workload.file");
        String logDir = params.getProperty("log.dir");
        boolean dropCache = Boolean.parseBoolean(params.getProperty("drop.cache"));
        String dropCachesSh = params.getProperty("drop.caches.sh");
        double workloadFileLength = (new File(workloadFilePath)).length();
        double readLength = 0;

        if (!logDir.endsWith("/"))
        {
            logDir += "/";
        }

        if (params.getProperty("method").equalsIgnoreCase("LOCAL"))
        {
            Configuration conf = new Configuration();
            try (BufferedReader workloadReader = new BufferedReader(new FileReader(workloadFilePath));
                 BufferedWriter timeWriter = new BufferedWriter(new FileWriter(logDir + "local_duration.csv"));
                 BufferedWriter columnWriter = new BufferedWriter(new FileWriter(logDir + "accessed_columns.txt")))
            {
                // get metadata
                FileStatus[] statuses = LocalEvaluator.getFileStatuses("hdfs://" + ConfigFactory.Instance().getProperty("namenode.host") + ":" +
                        ConfigFactory.Instance().getProperty("namenode.port") + tablePath, conf);
                ParquetMetadata[] metadatas = LocalEvaluator.getMetadatas(statuses, conf);

                timeWriter.write("\"query id\",\"duration(ms)\"\n");
                columnWriter.write("# Column index and name of accessed columns of each query in Parquet metadata.");

                String line;
                while ((line = workloadReader.readLine()) != null)
                {
                    readLength += line.length();
                    String columns = line.split("\t")[2];
                    String queryId = line.split("\t")[0];
                    // evaluate
                    // clear the caches and buffers
                    if (dropCache)
                    {
                        Runtime.getRuntime().exec(dropCachesSh);
                    }
                    LocalMetrics metrics = LocalEvaluator.execute(statuses, metadatas, columns.split(","), conf);

                    // log the results
                    timeWriter.write(queryId + "," + metrics.getTimeMillis() + "\n");
                    timeWriter.flush();
                    columnWriter.write("[query " + queryId + "]:\n");
                    for (Column column : metrics.getColumns())
                    {
                        columnWriter.write(column.getIndex() + "," + column.getName() + "\n");
                    }
                    columnWriter.write("\n\n");
                    columnWriter.flush();
                    progressListener.setPercentage(readLength/workloadFileLength);
                }

                results.setProperty("success", "true");
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate local error", e);
            }
        }
        else if (params.getProperty("method").equalsIgnoreCase("SPARK"))
        {
            String sparkMaster = ConfigFactory.Instance().getProperty("spark.master");
            String namenodeHost = ConfigFactory.Instance().getProperty("namenode.host");
            int namenodePort = Integer.valueOf(ConfigFactory.Instance().getProperty("namenode.port"));
            int appPort = Integer.parseInt(ConfigFactory.Instance().getProperty("spark.app.port"));
            int driverWebappsPort = Integer.parseInt(ConfigFactory.Instance().getProperty("spark.driver.webapps.port"));
            try (BufferedReader workloadReader = new BufferedReader(new FileReader(workloadFilePath));
                 BufferedWriter timeWriter = new BufferedWriter(new FileWriter(logDir + "spark_duration.csv")))
            {
                // get the column sizes
                ParquetMetadataStat stat = new ParquetMetadataStat(namenodeHost, namenodePort, tablePath);
                int n = stat.getFieldNames().size();
                List<String> names = stat.getFieldNames();
                double[] sizes = stat.getAvgColumnChunkSize();
                Map<String, Double> nameSizeMap = new HashMap<String, Double>();
                for (int j = 0; j < n; ++j)
                {
                    nameSizeMap.put(names.get(j).toLowerCase(), sizes[j]);
                }

                timeWriter.write("\"query id\",\"duration(ms)\"\n");

                // begin evaluate
                String line;
                int i = 0;
                while ((line = workloadReader.readLine()) != null)
                {
                    readLength += line.length();
                    String columns = line.split("\t")[2];
                    String queryId = line.split("\t")[0];

                    // get the smallest column as the order by column
                    String orderByColumn = null;
                    double size = Double.MAX_VALUE;

                    for (String name : columns.split(","))
                    {
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
                        Runtime.getRuntime().exec(dropCachesSh);
                    }
                    StageMetrics metrics = SparkEvaluator.execute("ordered_" + (i++) + queryId, sparkMaster, appPort, driverWebappsPort,
                            "hdfs://" + ConfigFactory.Instance().getProperty("namenode.host") + ":" +
                                    ConfigFactory.Instance().getProperty("namenode.port") + tablePath, columns, orderByColumn);

                    // log the results
                    timeWriter.write(queryId + "," + metrics.getDuration() + "\n");
                    timeWriter.flush();
                    progressListener.setPercentage(readLength/workloadFileLength);
                }
                results.setProperty("success", "true");

            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate local i/o error", e);
            } catch (MetadataException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate local metadata error", e);
            }

            if (receiver != null)
            {
                receiver.action(results);
            }
        }
    }
}
