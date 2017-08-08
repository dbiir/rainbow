package cn.edu.ruc.iir.rainbow.eva.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.ProgressListener;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.exception.MetaDataException;
import cn.edu.ruc.iir.rainbow.common.metadata.MetaDataStat;
import cn.edu.ruc.iir.rainbow.common.util.LogFactory;
import cn.edu.ruc.iir.rainbow.eva.LocalEvaluator;
import cn.edu.ruc.iir.rainbow.eva.SparkEvaluator;
import cn.edu.ruc.iir.rainbow.eva.domain.Column;
import cn.edu.ruc.iir.rainbow.eva.metrics.LocalMetrics;
import cn.edu.ruc.iir.rainbow.eva.metrics.StageMetrics;
import org.apache.commons.logging.Log;
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
    private Log log = LogFactory.Instance().getLog();

    private Receiver receiver = null;

    @Override
    public void setReceiver(Receiver receiver)
    {
        this.receiver = receiver;
    }

    /**
     * params should contain the following settings:
     * <ol>
     *   <li>method, LOCAL or SPARK, if LOCAL is used, the jar-ball contains
     *   this method must be run by hadoop -jar command so that the correct hadoop
     *   Configuration can be get and used to read files on HDFS.</li>
     *   <li>master, the hostname of spark master if method=SPARK</li>
     *   <li>ordered.table.dir, the path of ordered table directory on HDFS,
     *   should have the hdfs://namenode:port prefix</li>
     *   <li>table.dir, the path of unordered table directory on HDFS,
     *   should have the hdfs://namenode:port prefix</li>
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
        Properties results = new Properties();
        results.setProperty("success", "false");
        ProgressListener progressListener = percentage -> {
            if (receiver != null)
            {
                receiver.progress(percentage);
            }
        };
        progressListener.setPercentage(0.0);


        String orderedPath = params.getProperty("ordered.table.dir");
        String unorderedPath = params.getProperty("table.dir");
        String workloadFilePath = params.getProperty("workload.file");
        String log_dir = params.getProperty("log.dir");
        boolean dropCache = Boolean.parseBoolean(params.getProperty("drop.cache"));
        String dropCachesSh = params.getProperty("drop.caches.sh");
        double workloadFileLength = (new File(workloadFilePath)).length();
        double readLength = 0;

        if (params.getProperty("method").equalsIgnoreCase("local"))
        {


            Configuration conf = new Configuration();
            try (BufferedReader reader = new BufferedReader(new FileReader(workloadFilePath));
                 BufferedWriter timeWriter = new BufferedWriter(new FileWriter(log_dir + "local_time"));
                 BufferedWriter columnWriter = new BufferedWriter(new FileWriter(log_dir + "columns")))
            {
                // get metadata
                FileStatus[] orderedStatuses = LocalEvaluator.getFileStatuses(orderedPath, conf);
                FileStatus[] unorderedStatuses = LocalEvaluator.getFileStatuses(unorderedPath, conf);
                ParquetMetadata[] orderedMetadatas = LocalEvaluator.getMetadatas(orderedStatuses, conf);
                ParquetMetadata[] unorderedMetadatas = LocalEvaluator.getMetadatas(unorderedStatuses, conf);

                String line = null;
                int i = 0;
                while ((line = reader.readLine()) != null)
                {
                    readLength += line.length();
                    String columns = line.split("\t")[2];
                    // evaluate
                    // clear the caches and buffers
                    if (dropCache)
                    {
                        Runtime.getRuntime().exec(dropCachesSh);
                    }
                    LocalMetrics orderedMetrics = LocalEvaluator.execute(orderedStatuses, orderedMetadatas, columns.split(","), conf);
                    // clear the caches and buffers
                    if (dropCache)
                    {
                        Runtime.getRuntime().exec(dropCachesSh);
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
                    progressListener.setPercentage(readLength/workloadFileLength);
                }

                results.setProperty("success", "true");
                results.setProperty("log.dir", log_dir);
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate local error", e);
            }
        }
        else if (params.getProperty("method").equalsIgnoreCase("spark"))
        {
            String masterHostName = params.getProperty("master");
            try (BufferedReader reader = new BufferedReader(new FileReader(workloadFilePath));
                 BufferedWriter timeWriter = new BufferedWriter(new FileWriter(log_dir + "spark_time")))
            {
                // get the column sizes
                MetaDataStat stat = new MetaDataStat(masterHostName, 9000, orderedPath.split("9000")[1]);
                System.out.println(masterHostName);
                int n = stat.getFieldNames().size();
                List<String> names = stat.getFieldNames();
                double[] sizes = stat.getAvgColumnChunkSize();
                Map<String, Double> nameSizeMap = new HashMap<String, Double>();
                for (int j = 0; j < n; ++j)
                {
                    nameSizeMap.put(names.get(j).toLowerCase(), sizes[j]);
                }

                // begin evaluate
                String line;
                int i = 0;
                while ((line = reader.readLine()) != null)
                {
                    readLength += line.length();
                    String columns = line.split("\t")[2];
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
                    StageMetrics orderedMetrics = SparkEvaluator.execute("ordered_" + i, masterHostName, orderedPath, columns, orderByColumn);
                    // clear the caches and buffers
                    if (dropCache)
                    {
                        Runtime.getRuntime().exec(dropCachesSh);
                    }
                    StageMetrics unorderedMetrics = SparkEvaluator.execute("unordered_" + i, masterHostName, unorderedPath, columns, orderByColumn);

                    // log the results
                    timeWriter.write(i + "\t" + orderedMetrics.getDuration() + "\t" + unorderedMetrics.getDuration() + "\n");
                    timeWriter.flush();
                    ++i;
                    progressListener.setPercentage(readLength/workloadFileLength);
                }
                results.setProperty("success", "true");
                results.setProperty("log.dir", log_dir);

            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate local i/o error", e);
            } catch (MetaDataException e)
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
