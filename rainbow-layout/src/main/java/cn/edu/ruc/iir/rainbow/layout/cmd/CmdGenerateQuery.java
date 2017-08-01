package cn.edu.ruc.iir.rainbow.layout.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;

import java.util.Properties;

public class CmdGenerateQuery implements Command
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
     *   <li>table.type, ordered, dupped, origin</li>
     *   <li>file.format, orc, parquet, text</li>
     *   <li>schema.file</li>
     *   <li>ddl.file</li>
     * </ol>
     *
     * this method will pass the following results to receiver:
     * <ol>
     *   <li>ddl.file</li>
     * </ol>
     * @param params
     */
    @Override
    public void execute(Properties params)
    {
        String algoName = params.getProperty("algorithm.name");
        String schemaFilePath = params.getProperty("schema.file.path");
        String workloadFilePath = params.getProperty("workload.file.path");
        String duppedSchemaFilePath = params.getProperty("dupped.schema.file.path");
        String duppedWorkloadFilePath = params.getProperty("dupped.workload.file.path");

        Properties results = new Properties();
        //try
        //{

        results.setProperty("dupped.schema.file.path", duppedSchemaFilePath);
        results.setProperty("dupped.workload.file.path", duppedWorkloadFilePath);
        //}
        /* catch (IOException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "I/O error, check the file paths", e);
        } catch (InterruptedException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "interrupted while waiting for algorithm execution", e);
        } catch (ColumnNotFoundException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "column not fount when building workload", e);
        } catch (ClassNotFoundException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "algorithm class not fount", e);
        } catch (AlgoException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "algorithm initialization error", e);
        }*/

        if (this.receiver == null)
        {
            receiver.action(results);
        }
    }
}
