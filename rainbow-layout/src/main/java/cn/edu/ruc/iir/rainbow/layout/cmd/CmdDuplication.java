package cn.edu.ruc.iir.rainbow.layout.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.*;
import cn.edu.ruc.iir.rainbow.layout.algorithm.AlgorithmFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.DupAlgorithm;
import cn.edu.ruc.iir.rainbow.layout.algorithm.ExecutorContainer;
import cn.edu.ruc.iir.rainbow.common.cmd.ProgressListener;
import cn.edu.ruc.iir.rainbow.layout.builder.ColumnOrderBuilder;
import cn.edu.ruc.iir.rainbow.layout.builder.SimulatedSeekCostBuilder;
import cn.edu.ruc.iir.rainbow.layout.builder.WorkloadBuilder;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.seekcost.LinearSeekCostFunction;
import cn.edu.ruc.iir.rainbow.layout.seekcost.PowerSeekCostFunction;
import cn.edu.ruc.iir.rainbow.layout.seekcost.SeekCostFunction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by hank on 17-5-4.
 */
public class CmdDuplication implements Command
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
     *   <li>algorithm.name, name of the algorithm, configured in rainbow.properties</li>
     *   <li>schema.file</li>
     *   <li>workload.file</li>
     *   <li>dupped.schema.file</li>
     *   <li>dupped.workload.file</li>
     *   <li>seek.cost.function, should be one of linear, power, simulated, if it is not given, then power is applied</li>
     *   <li>seek.cost.file, if seek.cost.function is set to simulated, this param should be given</li>
     *   <li>computation.budget</li>
     * </ol>
     *
     * this method will pass the following results to receiver:
     * <ol>
     *   <li>init.cost, in milliseconds</li>
     *   <li>final.cost, in milliseconds</li>
     *   <li>dupped.schema.file</li>
     *   <li>dupped.workload.file</li>
     *   <li>success, true or false</li>
     * </ol>
     * @param params
     */
    @Override
    public void execute(Properties params)
    {
        Properties results = new Properties(params);
        results.setProperty("success", "false");
        if (this.receiver != null)
        {
            this.receiver.progress(0);
        }

        String algoName = params.getProperty("algorithm.name");
        String schemaFilePath = params.getProperty("schema.file");
        String workloadFilePath = params.getProperty("workload.file");
        String duppedSchemaFilePath = params.getProperty("dupped.schema.file");
        String duppedWorkloadFilePath = params.getProperty("dupped.workload.file");
        long budget = Long.parseLong(params.getProperty("computation.budget"));
        SeekCostFunction.Type funcType = SeekCostFunction.Type.valueOf(
                params.getProperty("seek.cost.function", SeekCostFunction.Type.POWER.name()).toUpperCase());
        SeekCostFunction seekCostFunction = null;

        switch (funcType)
        {
            case LINEAR:
                seekCostFunction = new LinearSeekCostFunction();
                break;
            case POWER:
                seekCostFunction = new PowerSeekCostFunction();
                break;
            case SIMULATED:
                try
                {
                    String seekCostFilePath = params.getProperty("seek.cost.file");
                    seekCostFunction = SimulatedSeekCostBuilder.build(new File(seekCostFilePath));
                } catch (IOException e)
                {
                    ExceptionHandler.Instance().log(ExceptionType.ERROR,
                            "get seek cost file error", e);
                }
                break;
            default:
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "unknown function type " + funcType,
                        new AlgoException("seek cost function type not supported"));
                if (receiver != null)
                {
                    receiver.action(results);
                }
                return;
        }

        try
        {
            List<Column> initColumnOrder = ColumnOrderBuilder.build(new File(schemaFilePath));
            List<Query> workload = WorkloadBuilder.build(new File(workloadFilePath), initColumnOrder);
            DupAlgorithm dupAlgo = (DupAlgorithm) AlgorithmFactory.Instance().getAlgorithm(algoName,
                    budget, new ArrayList<>(initColumnOrder), workload, seekCostFunction);

            try
            {
                ProgressListener progressListener = percentage -> {
                    if (this.receiver != null)
                    {
                        this.receiver.progress(percentage);
                    }
                };
                ExecutorContainer container = new ExecutorContainer(dupAlgo, 1);
                container.waitForCompletion(budget/100, progressListener);


            } catch (NotMultiThreadedException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "thread number is " + 1, e);
            }

            results.setProperty("init.cost", ""+dupAlgo.getSchemaSeekCost());
            results.setProperty("final.cost", ""+dupAlgo.getCurrentWorkloadSeekCost());
            WorkloadBuilder.saveAsWorkloadFile(new File(duppedWorkloadFilePath), dupAlgo.getWorkloadPattern());
            ColumnOrderBuilder.saveAsDDLSegment(new File(duppedSchemaFilePath), dupAlgo.getColumnOrder());

            results.setProperty("success", "true");
        } catch (IOException e)
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
        }

        if (this.receiver != null)
        {
            receiver.action(results);
        }
    }
}
