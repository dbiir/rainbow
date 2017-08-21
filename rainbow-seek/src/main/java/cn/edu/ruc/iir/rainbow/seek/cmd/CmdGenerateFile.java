package cn.edu.ruc.iir.rainbow.seek.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.ProgressListener;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.exception.ParameterNotSupportedException;
import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.seek.FileGenerator;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by hank on 16-12-25.
 */
public class CmdGenerateFile implements Command
{
    private FileGenerator generator = null;
    private Receiver receiver = null;

    public CmdGenerateFile()
    {
        this.generator = FileGenerator.Instance();
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
     *   <li>data.path, the directory on HDFS or local file system to store the generated files</li>
     *   <li>block.size, the HDFS block size</li>
     *   <li>num.block,  the number of blocks</li>
     * </ol>
     * A lot of files will be generated under data path, with each file only containing one block.
     * Each file uses an unique integer number (NO) as the file name.
     *
     * This method will pass the following results to receiver:
     * <ol>
     *   <li>success, true or false</li>
     *   <li>data.path</li>
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

        //generate the test file
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
        long blockSize = Long.parseLong(params.getProperty("block.size"));
        long numBlocks = Long.parseLong(params.getProperty("num.blocks"));

        if (params.getProperty("method").equalsIgnoreCase("HDFS"))
        {
            // generate hdfs file
            try
            {
                generator.generateHDFSFile(blockSize, numBlocks, dataPath, progressListener);
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "generate HDFS file error", e);
            }
            results.setProperty("success", "true");
        } else if (params.getProperty("method").equalsIgnoreCase("LOCAL"))
        {
            //generate local file
            try
            {
                generator.generateLocalFile(blockSize, numBlocks, dataPath, progressListener);
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "generate local file error", e);
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
