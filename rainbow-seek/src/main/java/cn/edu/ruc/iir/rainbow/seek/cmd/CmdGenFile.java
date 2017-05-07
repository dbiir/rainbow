package cn.edu.ruc.iir.rainbow.seek.cmd;

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
public class CmdGenFile implements Command
{
    private FileGenerator generator = null;
    private Receiver receiver = null;

    public CmdGenFile ()
    {
        this.generator = FileGenerator.Instance();
    }

    @Override
    public void setReceiver(Receiver receiver)
    {
        this.receiver = receiver;
    }

    /**
     *
     * @param params --method hdfs --dir dirPath --block-size blockSize --num-block numBlock /
     *               --method local --file-path filePath --file-size Fileize
     */
    public void execute(Properties params)
    {
        //generate the test file
        if (params.getProperty("method") == null)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "method is null. Exiting...",
                    new NullPointerException());
            return;
        }

        if (params.getProperty("--method").equalsIgnoreCase("hdfs"))
        {
            // generate hdfs file
            String dirPath = params.getProperty("--dir");
            long blockSize = Long.parseLong(params.getProperty("--block-size"));
            long numBlock = Long.parseLong(params.getProperty("--num-block"));
            try
            {
                generator.generateHDFSFile(blockSize, numBlock, dirPath);
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "generate hdfs file error", e);
            }
        } else if (params.getProperty("--method").equalsIgnoreCase("local"))
        {
            //generate local file
            long FileSize = Long.parseLong(params.getProperty("--file-size"));
            String filePath = params.getProperty("--file-path");
            try
            {
                generator.generateLocalFile(FileSize, filePath);
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "generate local file error", e);
            }
        } else
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, params.getProperty("--method") +
                    " not supported. Exiting...", new ParameterNotSupportedException(params.getProperty("--method")));
            return;
        }
    }
}
