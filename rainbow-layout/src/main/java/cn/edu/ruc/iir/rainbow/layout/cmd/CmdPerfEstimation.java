package cn.edu.ruc.iir.rainbow.layout.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.layout.builder.ColumnOrderBuilder;
import cn.edu.ruc.iir.rainbow.layout.builder.SimulatedSeekCostBuilder;
import cn.edu.ruc.iir.rainbow.layout.builder.WorkloadBuilder;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.seekcost.LinearSeekCostFunction;
import cn.edu.ruc.iir.rainbow.layout.seekcost.PowerSeekCostFunction;
import cn.edu.ruc.iir.rainbow.layout.seekcost.SeekCostFunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class CmdPerfEstimation implements Command
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
     *   <li>schema.file</li>
     *   <li>workload.file</li>
     *   <li>num.row.group</li>
     *   <li>seek.cost.function, should be one of linear, power, simulated, if it is not given, then power is applied</li>
     *   <li>seek.cost.file, if seek.cost.function is set to simulated, this param should be given</li>
     *   <li>log.file the local directory used to write evaluation results, must end with '/'</li>
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

        String schemaFilePath = params.getProperty("schema.file");
        String workloadFilePath = params.getProperty("workload.file");
        List<Column> columnOrder = null;
        List<Query> workload = null;
        try
        {
            columnOrder = ColumnOrderBuilder.build(new File(schemaFilePath));
            workload = WorkloadBuilder.build(new File(workloadFilePath), columnOrder);
        } catch (ColumnNotFoundException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "column not found when building column order.", e);
            if (receiver != null)
            {
                receiver.action(results);
            }
            return;
        } catch (IOException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "i/o error when building workload and column order.", e);
            if (receiver != null)
            {
                receiver.action(results);
            }
            return;
        }

        SeekCostFunction.Type funcType = SeekCostFunction.Type.valueOf(
                params.getProperty("seek.cost.function", SeekCostFunction.Type.POWER.name()).toUpperCase());
        SeekCostFunction seekCostFunction = null;

        if (funcType == SeekCostFunction.Type.LINEAR)
        {
            seekCostFunction = new LinearSeekCostFunction();
        } else if (funcType == SeekCostFunction.Type.POWER)
        {
            seekCostFunction = new PowerSeekCostFunction();
        } else if (funcType == SeekCostFunction.Type.SIMULATED)
        {
            try
            {
                String seekCostFilePath = params.getProperty("seek.cost.file");
                seekCostFunction = SimulatedSeekCostBuilder.build(new File(seekCostFilePath));
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR,
                        "get seek cost file error", e);
                if (receiver != null)
                {
                    receiver.action(results);
                }
                return;
            }
        }

        String logFile = params.getProperty("log.file");
        int rowGroupNum = Integer.parseInt(params.getProperty("num.row.group"));
        int taskInitMs = Integer.parseInt(ConfigFactory.Instance().getProperty("node.task.init.ms"));
        int numMapSlots = Integer.parseInt(ConfigFactory.Instance().getProperty("node.map.slots"));
        int diskBandwidth = Integer.parseInt(ConfigFactory.Instance().getProperty("node.disk.bandwidth"));

        try (BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile)))
        {
            logWriter.write("\"query id\",\"duration (ms)\"\n");
            logWriter.flush();
            for (Query query : workload)
            {
                double seekCost = this.getQuerySeekCost(columnOrder, query, seekCostFunction) * rowGroupNum;
                double taskInitCost = rowGroupNum * taskInitMs / numMapSlots;
                double readSize = 0;
                for (Column column : columnOrder)
                {
                    if (query.getColumnIds().contains(column.getId()))
                    {
                        readSize += column.getSize();
                    }
                }
                readSize *= rowGroupNum;
                logWriter.write(query.getSid() + "," + (seekCost + taskInitCost + readSize / diskBandwidth));
                logWriter.newLine();
                logWriter.flush();
            }
        } catch (IOException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "i/o error when writing logs.", e);
            if (receiver != null)
            {
                receiver.action(results);
            }
            return;
        }
    }

    /**
     * get the seek cost of a query (on the given column order).
     * @param columnOrder
     * @param query
     * @param seekCostFunction
     * @return
     */
    private double getQuerySeekCost(List<Column> columnOrder, Query query, SeekCostFunction seekCostFunction)
    {
        double querySeekCost = 0, seekDistance = 0;
        int accessedColumnNum = 0;
        for (int i = columnOrder.size() - 1; i >= 0; --i)
        {
            if (query.getColumnIds().contains(columnOrder.get(i).getId()))
            {
                // column i has been accessed by the query
                querySeekCost += seekCostFunction.calculate(seekDistance);
                seekDistance = 0;
                ++accessedColumnNum;
                if (accessedColumnNum >= query.getColumnIds().size())
                {
                    // the query has accessed all the necessary columns
                    break;
                }
            } else
            {
                // column i has been skipped (seek over) by the query
                seekDistance += columnOrder.get(i).getSize();
            }
        }
        return querySeekCost;
    }
}
