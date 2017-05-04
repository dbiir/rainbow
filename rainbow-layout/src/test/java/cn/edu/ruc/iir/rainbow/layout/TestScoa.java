package cn.edu.ruc.iir.rainbow.layout;

import cn.edu.ruc.iir.rainbow.layout.algorithm.Algorithm;
import cn.edu.ruc.iir.rainbow.layout.algorithm.AlgorithmFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.ExecutorContainer;
import cn.edu.ruc.iir.rainbow.layout.builder.ColumnOrderBuilder;
import cn.edu.ruc.iir.rainbow.layout.builder.WorkloadBuilder;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.seekcost.PowerSeekCostFunction;
import cn.edu.ruc.iir.rainbow.layout.seekcost.SeekCostFunction;
import cn.edu.ruc.iir.rainbow.common.exception.*;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hank on 2015/4/28.
 */
public class TestScoa
{
    @Test
    public void testScoa() throws IOException, ColumnNotFoundException, AlgoException, ClassNotFoundException, InterruptedException
    {
        List<Column> initColumnOrder = ColumnOrderBuilder.build(new File(TestScoa.class.getResource("/schema.txt").getFile()));
        List<Query> workload = WorkloadBuilder.build(new File(TestScoa.class.getResource("/workload.txt").getFile()), initColumnOrder);
        SeekCostFunction seekCostFunction = new PowerSeekCostFunction();
        //SimulatedSeekCostBuilder.build(new File("cord-generator/resources/seek_cost.txt"));

        Algorithm fastScoa = AlgorithmFactory.Instance().getAlgorithm("scoa", 10, new ArrayList<>(initColumnOrder), workload, seekCostFunction);
        System.out.println("Init cost: " + fastScoa.getSchemaSeekCost());
        try
        {
            ExecutorContainer container = new ExecutorContainer(fastScoa, 1);
            container.waitForCompletion();
        } catch (NotMultiThreadedException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "thread number is " + 1, e);
        }

        System.out.println("Final cost: " + fastScoa.getCurrentWorkloadSeekCost());
        ColumnOrderBuilder.saveAsDDLSegment(new File(TestScoa.class.getResource("/scoa_ordered_schema.txt").getFile()), fastScoa.getColumnOrder());
    }
}
