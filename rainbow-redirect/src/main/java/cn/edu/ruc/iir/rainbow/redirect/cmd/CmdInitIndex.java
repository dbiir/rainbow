package cn.edu.ruc.iir.rainbow.redirect.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.redirect.builder.PatternBuilder;
import cn.edu.ruc.iir.rainbow.redirect.index.Index;
import cn.edu.ruc.iir.rainbow.redirect.index.IndexFactory;
import cn.edu.ruc.iir.rainbow.redirect.index.Inverted;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CmdInitIndex implements Command
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
     *   <li>dupped.schema.file.path</li>
     *   <li>dupped.workload.file.path</li>
     * </ol>
     * this method will pass the following results to receiver:
     * <ol>
     *   <li>cache.index.success, true or false</li>
     * </ol>
     * @param params
     */
    @Override
    public void execute(Properties params)
    {
        String schemaFilePath = params.getProperty("dupped.schema.file.path");
        String workloadFilePath = params.getProperty("dupped.workload.file.path");
        Properties results = new Properties();

        try (BufferedReader reader = new BufferedReader(new FileReader(schemaFilePath)))
        {
            // create the dupped column order.
            List<String> columnOrder = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null)
            {
                String[] tokens = line.split("\t");
                columnOrder.add(tokens[0]);
            }


            Index index = new Inverted(columnOrder, PatternBuilder.build(new File(workloadFilePath)));
            IndexFactory.Instance().cacheIndex(
                    ConfigFactory.Instance().getProperty("inverted.index.name"), index);
            results.setProperty("cache.index.success", "true");
        } catch (FileNotFoundException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "error when creating dupped schem file reader", e);
        } catch (IOException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "error when closing dupped schem file reader", e);
        } catch (ColumnNotFoundException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "error when building workload pattern", e);// build pattern
        }

        if (this.receiver == null)
        {
            receiver.action(results);
        }
    }
}
