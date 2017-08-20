package cn.edu.ruc.iir.rainbow.seek.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.ProgressListener;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.exception.ParameterNotSupportedException;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.HDFSInputFactory;
import cn.edu.ruc.iir.rainbow.common.util.InputFactory;
import cn.edu.ruc.iir.rainbow.seek.SeekPerformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by hank on 16-12-25.
 */
public class CmdSeekEvaluation implements Command
{
    private SeekPerformer seekPerformer = null;
    private Receiver receiver = null;

    public CmdSeekEvaluation()
    {
        this.seekPerformer = SeekPerformer.Instance();
    }

    @Override
    public void setReceiver(Receiver receiver)
    {
        this.receiver = receiver;
    }

    /**
     * params should contain the following settings:
     * <ol>
     *   <li>method, HDFS or LOCAL</li>
     *   <li>data.path, the directory on HDFS to store the generated files if method=HDFS</li>
     *   <li>num.seeks, number of seeks to be performed in each segment</li>
     *   <li>seek.distance.interval, the interval of seek distance in bytes</li>
     *   <li>num.intervals, # the number of seek distance intervals in the seek cost function</li>
     *   <li>read.length,  the length in bytes been read after each seek</li>
     *   <li>num.skipped.files, the number of files been skipped between two segments if method=HDFS</li>
     *   <li>log.dir, the local directory used to write evaluation results.
     *   This directory can not be exist and will be created by this method itself</li>
     *   <li>start.file.num, the integer number of HDFS file the seek evaluation starts if method=HDFS</li>
     *   <li>end.file.num, the integer number of HDFS file the seek evaluation ends if method=HDFS</li>
     *   <li>drop.caches.sh, the file path of drop_caches.sh</li>
     * </ol>
     * this method will pass the following results to receiver:
     * <ol>
     *   <li>success, true or false</li>
     *   <li>log.dir</li>
     * </ol>
     * @param params
     */
    public void execute(Properties params)
    {
        Properties results = new Properties(params);
        results.setProperty("success", "false");
        ProgressListener progressListener = percentage -> {
            if (this.receiver != null)
            {
                this.receiver.progress(percentage);
            }
        };
        progressListener.setPercentage(0.0);

        // test the seek cost
        if (params.getProperty("method") == null)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "method is null. Exiting...",
                    new NullPointerException());
            if (receiver != null)
            {
                receiver.action(results);
            }
            return;
        }
        String dataPath = params.getProperty("data.path");
        int numSeeks = Integer.parseInt(params.getProperty("num.seeks"));
        int numSkippedFiles = Integer.parseInt(params.getProperty("num.skipped.files"));
        long seekDistInterval = Long.parseLong(params.getProperty("seek.distance.interval"));
        int numInvervals = Integer.parseInt(params.getProperty("num.intervals"));
        int readLength = Integer.parseInt(params.getProperty("read.length"));
        String logDir = params.getProperty("log.dir");
        int startFileNumber = Integer.parseInt(params.getProperty("start.file.num"));
        int endFileNumber = Integer.parseInt(params.getProperty("end.file.num"));
        String dropCachesSh = params.getProperty("drop.caches.sh");
        if (logDir.charAt(logDir.length()-1) != '/' && logDir.charAt(logDir.length()-1) != '\\')
        {
            logDir += "/";
        }
        File dir = new File(logDir);
        if (dir.exists())
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "log directory " + logDir +
                    " exists. Exiting...", new ParameterNotSupportedException(logDir));
            if (receiver != null)
            {
                receiver.action(results);
            }
            return;
        }

        dir.mkdirs();

        if (params.getProperty("method").equalsIgnoreCase("HDFS"))
        {
            //evaluate hdfs seek cost
            Configuration conf  = new Configuration();
            String namenode = ConfigFactory.Instance().getProperty("namenode.host") + ":" +
                    ConfigFactory.Instance().getProperty("namenode.port");
            try
            {
                FileStatus[] statuses = HDFSInputFactory.Instance().getFileStatuses("hdfs://" + namenode + dataPath, conf);

                BufferedWriter seekCostWriter = new BufferedWriter(new FileWriter(logDir + "seek_cost.txt"));
                seekCostWriter.write(seekDistInterval + "\n");
                seekCostWriter.write("0\t0\n");

                for (int j = 1; j <= numInvervals; ++j)
                {
                    long seekDistance = seekDistInterval * j;

                    // drop caches before seeks.
                    Runtime.getRuntime().exec(dropCachesSh);

                    File subdir = new File(logDir + "seek_dist_" + seekDistance);
                    subdir.mkdir();

                    BufferedWriter summaryWriter = new BufferedWriter(new FileWriter(subdir.getPath() + "/summary.csv"));
                    summaryWriter.write("\"file number\",\t\"seek distance\",\t\"read length\",\t\"seek time(us)\"\n");
                    double totalSeekCost = 0;

                    for (int i = startFileNumber; i < endFileNumber; i += numSkippedFiles)
                    {
                        // get the hdfs file status
                        FileStatus status = statuses[i];
                        long[] seekCosts = new long[numSeeks];
                        int realNumSeeks = seekPerformer.performHDFSSeeks(
                                seekCosts, status, numSeeks, readLength, seekDistance, conf);
                        double fileMedCost = seekPerformer.logSeekCost(seekCosts, realNumSeeks, subdir.getPath() + "/file_num_" + i + ".txt");
                        summaryWriter.write(i + "," + seekDistance + "," + readLength + "," + fileMedCost + "\n");
                        totalSeekCost += fileMedCost/1000;
                    }
                    summaryWriter.flush();
                    summaryWriter.close();

                    double avgSeekCost = totalSeekCost/((endFileNumber-startFileNumber)/numSkippedFiles);
                    // seek cost in seek_cost.txt is in ms
                    seekCostWriter.write(seekDistance + "\t" + avgSeekCost + "\n");
                    seekCostWriter.flush();

                    progressListener.setPercentage(1.0 * j / numInvervals);
                }
                seekCostWriter.close();
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "hdfs seek evaluation error", e);
            }
            results.setProperty("success", "true");
        } else if (params.getProperty("method").equalsIgnoreCase("LOCAL"))
        {
            //test local seek cost
            try
            {
                File[] files = InputFactory.Instance().getFiles(dataPath);

                BufferedWriter seekCostWriter = new BufferedWriter(new FileWriter(logDir + "seek_cost.txt"));
                seekCostWriter.write(seekDistInterval + "\n");
                seekCostWriter.write("0\t0\n");

                for (int j = 1; j <= numInvervals; ++j)
                {
                    long seekDistance = seekDistInterval * j;

                    // drop caches before seeks.
                    Runtime.getRuntime().exec(dropCachesSh);

                    File subdir = new File(logDir + "seek_dist_" + seekDistance);
                    subdir.mkdir();

                    BufferedWriter summaryWriter = new BufferedWriter(new FileWriter(subdir.getPath() + "/summary.csv"));
                    summaryWriter.write("\"file number\",\t\"seek distance\",\t\"read length\",\t\"seek time(us)\"\n");
                    double totalSeekCost = 0;

                    for (int i = startFileNumber; i < endFileNumber; i += numSkippedFiles)
                    {
                        // get the local file
                        File file = files[i];
                        long[] seekCosts = new long[numSeeks];
                        int realNumSeeks = seekPerformer.performLocalSeeks(
                                seekCosts, file, numSeeks, readLength, seekDistance);
                        double fileMedCost = seekPerformer.logSeekCost(seekCosts, realNumSeeks, subdir.getPath() + "/file_num_" + i + ".txt");
                        summaryWriter.write(i + "," + seekDistance + "," + readLength + "," + fileMedCost + "\n");
                        totalSeekCost += fileMedCost/1000;
                    }
                    summaryWriter.flush();
                    summaryWriter.close();

                    double avgSeekCost = totalSeekCost/((endFileNumber-startFileNumber)/numSkippedFiles);
                    // seek cost in seek_cost.txt is in ms
                    seekCostWriter.write(seekDistance + "\t" + avgSeekCost + "\n");
                    seekCostWriter.flush();

                    progressListener.setPercentage(1.0 * j / numInvervals);
                }
                seekCostWriter.close();
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "local seek evaluation error", e);
            }
            results.setProperty("success", "true");
        } else
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, params.getProperty("method") +
                    " not supported. Exiting...", new ParameterNotSupportedException(params.getProperty("method")));
        }

        if (receiver != null)
        {
            receiver.action(results);
        }
    }
}
