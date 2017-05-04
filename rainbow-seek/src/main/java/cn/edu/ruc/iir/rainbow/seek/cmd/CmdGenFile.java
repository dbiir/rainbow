package cn.edu.ruc.iir.rainbow.seek.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.exception.ParameterNotSupportedException;
import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.seek.FileGenerator;

import java.io.IOException;

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
     * @param params hdfs dirPath blockSize numBlock / local filePath Fileize
     */
    public void execute(String[] params)
    {
        //generate the test file
        if ((!params[0].equalsIgnoreCase("hdfs")) && (!params[0].equalsIgnoreCase("local")))
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, params[0] +
                    " not supported. Exiting...", new ParameterNotSupportedException(params[0]));
            return;
        }

        if (params[0].equalsIgnoreCase("hdfs"))
        {
            // generate hdfs file
            String dirPath = params[1];
            long blockSize = Long.parseLong(params[2]);
            long numBlock = Long.parseLong(params[3]);
            try
            {
                generator.generateHDFSFile(blockSize, numBlock, dirPath);
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "generate hdfs file error", e);
            }
        } else if (params[0].equalsIgnoreCase("local"))
        {
            //generate local file
            long FileSize = Long.parseLong(params[2]);
            String filePath = params[1];
            try
            {
                generator.generateLocalFile(FileSize, filePath);
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "generate local file error", e);
            }
        }
    }
}
