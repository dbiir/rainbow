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
     *   <li>dir, the directory on HDFS to store the generated files if method=HDFS</li>
     *   <li>file, the local file path to store the generated data if method=LOCAL</li>
     *   <li>block.size, the HDFS block size if method=HDFS</li>
     *   <li>num.block,  the number of blocks if method=HDFS</li>
     *   <li>file.size the size of the generated file if method=LOCAL</li>
     * </ol>
     * For HDFS, a lot of files will be generated, with each file only containing one block.
     * Each file generated on HDFS use an unique integer number (NO) as the file name.
     *
     * This method will pass the following results to receiver:
     * <ol>
     *   <li>success, true or false</li>
     *   <li>dir, if method=HDFS</li>
     *   <li>file, if method=LOCAL</li>
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

        if (params.getProperty("method").equalsIgnoreCase("hdfs"))
        {
            // generate hdfs file
            String dirPath = params.getProperty("dir");
            long blockSize = Long.parseLong(params.getProperty("block.size"));
            long numBlock = Long.parseLong(params.getProperty("num.block"));
            try
            {
                generator.generateHDFSFile(blockSize, numBlock, dirPath, progressListener);
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "generate hdfs file error", e);
            }
            results.setProperty("success", "true");
            results.setProperty("dir", dirPath);
        } else if (params.getProperty("method").equalsIgnoreCase("local"))
        {
            //generate local file
            long FileSize = Long.parseLong(params.getProperty("file.size"));
            String filePath = params.getProperty("file");
            try
            {
                generator.generateLocalFile(FileSize, filePath, progressListener);
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "generate local file error", e);
            }
            results.setProperty("success", "true");
            results.setProperty("file", filePath);
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
