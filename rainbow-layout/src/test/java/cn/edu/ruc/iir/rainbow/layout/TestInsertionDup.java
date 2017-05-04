package cn.edu.ruc.iir.rainbow.layout;

import cn.edu.ruc.iir.rainbow.layout.algorithm.AlgorithmFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.DupAlgorithm;
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
 * Created by hank on 2017/4/28.
 */
public class TestInsertionDup
{
    @Test
    public void testInsertionDup() throws IOException, ColumnNotFoundException, AlgoException, ClassNotFoundException, InterruptedException
    {
        List<Column> initColumnOrder = ColumnOrderBuilder.build(new File(TestInsertionDup.class.getResource("/ordered_schema_1.txt").getFile()));
        List<Query> workload = WorkloadBuilder.build(new File(TestInsertionDup.class.getResource("/workload.txt").getFile()), initColumnOrder);
        SeekCostFunction seekCostFunction = new PowerSeekCostFunction();//SimulatedSeekCostBuilder.build(new File("cord-generator/resources/seek_cost.txt"));

        DupAlgorithm dup = (DupAlgorithm) AlgorithmFactory.Instance().getAlgorithm("insertion", 100, new ArrayList<>(initColumnOrder), workload, seekCostFunction);
        try
        {
            ExecutorContainer container = new ExecutorContainer(dup, 1);
            container.waitForCompletion();
        } catch (NotMultiThreadedException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "thread number is " + 1, e);
        }

        WorkloadBuilder.saveAsWorkloadFile(new File(TestInsertionDup.class.getResource("/duped_jobs.txt").getFile()), dup.getWorkloadPattern());
        ColumnOrderBuilder.saveAsDDLSegment(new File(TestInsertionDup.class.getResource("/duped_schema.txt").getFile()), dup.getColumnOrder());
    }
}
