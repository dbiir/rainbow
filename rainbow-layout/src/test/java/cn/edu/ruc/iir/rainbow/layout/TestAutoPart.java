package cn.edu.ruc.iir.rainbow.layout;

import cn.edu.ruc.iir.rainbow.common.exception.AlgoException;
import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.common.exception.NotMultiThreadedException;
import cn.edu.ruc.iir.rainbow.layout.algorithm.Algorithm;
import cn.edu.ruc.iir.rainbow.layout.algorithm.AlgorithmFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.ExecutorContainer;
import cn.edu.ruc.iir.rainbow.layout.builder.ColumnOrderBuilder;
import cn.edu.ruc.iir.rainbow.layout.builder.WorkloadBuilder;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.seekcost.PowerSeekCostFunction;
import cn.edu.ruc.iir.rainbow.layout.seekcost.SeekCostFunction;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by hank on 17-5-3.
 */
public class TestAutoPart
{
    @Test
    public void testAutoPart() throws IOException, ColumnNotFoundException, AlgoException, ClassNotFoundException, NotMultiThreadedException, InterruptedException
    {
        List<Column> initColumnOrder = ColumnOrderBuilder.build(new File(TestAutoPart.class.getResource("/schema.txt").getFile()));
        List<Query> workload = WorkloadBuilder.build(new File(TestAutoPart.class.getResource("/workload.txt").getFile()), initColumnOrder);

        SeekCostFunction seekCostFunction = new PowerSeekCostFunction();

        Algorithm algo = AlgorithmFactory.Instance().getAlgorithm("autopart", 5, initColumnOrder, workload, seekCostFunction);
        double initSeekCost = algo.getSchemaSeekCost();
        ExecutorContainer container = new ExecutorContainer(algo, 1);
        container.waitForCompletion();

        double finalSeekCost = algo.getCurrentWorkloadSeekCost();
        ColumnOrderBuilder.saveAsDDLSegment(new File(TestAutoPart.class.getResource("/").getFile() + "autopart_ordered_schema.txt"), algo.getColumnOrder());
        System.out.println(initSeekCost + ", " + finalSeekCost);
        System.exit(0);
    }
}
