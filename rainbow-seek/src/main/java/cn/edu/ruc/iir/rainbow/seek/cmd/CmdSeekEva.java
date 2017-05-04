package cn.edu.ruc.iir.rainbow.seek.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.exception.ParameterNotSupportedException;
import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.util.HDFSInputFactory;
import cn.edu.ruc.iir.rainbow.seek.SeekPerformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by hank on 16-12-25.
 */
public class CmdSeekEva implements Command
{
    private SeekPerformer seekPerformer = null;
    private Receiver receiver = null;

    public CmdSeekEva ()
    {
        this.seekPerformer = SeekPerformer.Instance();
    }

    @Override
    public void setReceiver(Receiver receiver)
    {
        this.receiver = receiver;
    }

    /**
     *
     * @param params hdfs pathOfFiles numSeeks seekDistance readLength skipFileNum LogDir [startFileNum] [endFileNum] /
     *               local pathOfFiles numSeeks seekDistance readLength skipDistance LogDir [startFileNum] [endFileNum]
     */
    public void execute(String[] params)
    {
        // test the seek cost
        if ((!params[1].equalsIgnoreCase("hdfs")) && (!params[1].equalsIgnoreCase("local")))
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, params[0] +
                    " not supported. Exiting...", new ParameterNotSupportedException(params[0]));
            return;
        }
        String path = params[1];
        int numSeeks = Integer.parseInt(params[2]);
        long seekDistance = Long.parseLong(params[3]);
        int readLength = Integer.parseInt(params[4]);
        String logDir = params[6];
        if (logDir.charAt(logDir.length()-1) != '/' && logDir.charAt(logDir.length()-1) != '\\')
        {
            logDir += "/";
        }
        File dir = new File(logDir);
        if (dir.exists())
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "log directory " + logDir +
                    " exists. Exiting...", new ParameterNotSupportedException(logDir));
            System.exit(1);
        }
        dir.mkdirs();
        if (params[0].equalsIgnoreCase("hdfs"))
        {
            //test hdfs seek cost
            int skipFileNum = Integer.parseInt(params[5]);
            Configuration conf  = new Configuration();
            try
            {
                FileStatus[] statuses = HDFSInputFactory.Instance().getFileStatuses(path, conf);
                BufferedWriter writer = new BufferedWriter(new FileWriter(logDir + "aggregate"));
                writer.write("fileNumber\tseekDistance\treadSize\tfileSeekTime(us)\n");
                System.out.print((seekDistance / 1024));
                int startFileNumber = 0, endFileNumber = statuses.length;
                if (params.length > 7)
                {
                    startFileNumber = Integer.parseInt(params[7]);
                    endFileNumber = Integer.parseInt(params[8]);
                }

                for (int i = startFileNumber; i < endFileNumber; i += skipFileNum)
                {
                    //get the hdfs file status
                    FileStatus status = statuses[i];
                    long[] seekCosts = new long[numSeeks];
                    int realNumSeeks = seekPerformer.hdfsSeekTest(seekCosts, status, numSeeks, readLength, seekDistance, conf);
                    double fileMidCost = seekPerformer.logSeekCost(seekCosts, realNumSeeks, logDir + "details-of-file-" + i);
                    writer.write(i + "\t" + seekDistance + "\t" + readLength + "\t" + fileMidCost + "\n");
                    System.out.print("\t" + fileMidCost);
                }
                writer.flush();
                writer.close();
                System.out.println();

            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "hdfs seek evaluation error", e);
            }
        } else if (params[0].equalsIgnoreCase("local"))
        {
            //test local seek cost
            long skipDistance = Long.parseLong(params[5]);

            try
            {
                File file = new File(path);
                long initPos = 0, endPos = file.length();
                BufferedWriter writer = new BufferedWriter(new FileWriter(logDir + "aggregate"));
                writer.write("initPos\tseekDistance\treadSize\tsegSeekTime(us)\n");
                System.out.print((seekDistance / 1024));
                if (params.length > 7)
                {
                    initPos = Long.parseLong(params[7]);
                    endPos = Long.parseLong(params[8]);
                }
                while (initPos < endPos)
                {
                    long[] seekCosts = new long[numSeeks];
                    int realNumSeeks = seekPerformer.localSeekTest(seekCosts, file, initPos, numSeeks, readLength, seekDistance);
                    double segMidCost = seekPerformer.logSeekCost(seekCosts, realNumSeeks, logDir + "details-of-seg-" + initPos);
                    writer.write(initPos + "\t" + seekDistance + "\t" + readLength + "\t" + segMidCost + "\n");

                    System.out.print("\t" + segMidCost);
                    initPos += skipDistance;
                }
                writer.flush();
                writer.close();
                System.out.println();

            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "local seek evaluation error", e);
            }
        }
    }
}
