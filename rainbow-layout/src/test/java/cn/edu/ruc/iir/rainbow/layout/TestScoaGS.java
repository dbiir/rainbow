package cn.edu.ruc.iir.rainbow.layout;

import cn.edu.ruc.iir.rainbow.common.exception.*;
import cn.edu.ruc.iir.rainbow.layout.algorithm.AlgorithmFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.ExecutorContainer;
import cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord.FastScoaGS;
import cn.edu.ruc.iir.rainbow.layout.builder.ColumnOrderBuilder;
import cn.edu.ruc.iir.rainbow.layout.builder.WorkloadBuilder;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.seekcost.PowerSeekCostFunction;
import cn.edu.ruc.iir.rainbow.layout.seekcost.SeekCostFunction;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestScoaGS
{
    @Test
    public void test () throws IOException, ColumnNotFoundException, AlgoException, ClassNotFoundException, InterruptedException
    {
        List<Column> initColumnOrder = ColumnOrderBuilder.build(new File(TestScoaGS.class.getResource("/schema.txt").getFile()));
        List<Query> workload = WorkloadBuilder.build(new File(TestScoaGS.class.getResource("/workload.txt").getFile()), initColumnOrder);
        SeekCostFunction seekCostFunction = new PowerSeekCostFunction();
        //SimulatedSeekCostBuilder.build(new File("cord-generator/resources/seek_cost.txt"));

        FastScoaGS fastScoa = (FastScoaGS) AlgorithmFactory.Instance().getAlgorithm("scoa.gs", 200, new ArrayList<>(initColumnOrder), workload, seekCostFunction);
        fastScoa.setNumRowGroups(100);
        fastScoa.setRowGroupSize(1024l*1024l*128l);
        fastScoa.setNumMapSlots(4);
        fastScoa.setTotalMemory(1024l*1024l*1024l*8l);
        fastScoa.setTaskInitMs(10);

        System.out.println("Init seek cost: " + fastScoa.getSchemaSeekCost() * fastScoa.getNumRowGroups());
        System.out.println("Init overhead: " + fastScoa.getSchemaOverhead());
        try
        {
            ExecutorContainer container = new ExecutorContainer(fastScoa, 1);
            container.waitForCompletion(1, percentage -> {
                System.out.println(percentage);
            });
        } catch (NotMultiThreadedException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "thread number is " + 1, e);
        }

        System.out.println("Final seek cost: " + (fastScoa.getCurrentWorkloadSeekCost() * fastScoa.getNumRowGroups()));
        System.out.println("Final overhead: " + fastScoa.getCurrentOverhead());
        System.out.println("Final number of row groups: " + fastScoa.getNumRowGroups());
        System.out.println("Final row group size： " + fastScoa.getRowGroupSize());
        ColumnOrderBuilder.saveAsSchemaFile(new File(TestScoaGS.class.getResource("/").getFile() + "scoa_ordered_schema.txt"), fastScoa.getColumnOrder());
        System.out.println("ordered schema file: " + TestScoaGS.class.getResource("/").getFile() + "scoa_ordered_schema.txt");
    }
}
