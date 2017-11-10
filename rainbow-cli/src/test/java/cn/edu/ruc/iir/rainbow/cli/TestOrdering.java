package cn.edu.ruc.iir.rainbow.cli;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import org.junit.jupiter.api.Test;

import java.util.Properties;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.cli
 * @ClassName: TestOrdering
 * @Description: test ordering
 * @author: Tao
 * @date: Create in 2017-08-13 11:37
 **/
public class TestOrdering {

    @Test
    public void test() {
        Invoker invoker = InvokerFactory.Instance().getInvoker(INVOKER.ORDERING);
        Properties params = new Properties();
        params.setProperty("algorithm.name", "scoa.gs");
        params.setProperty("workload.file", "H:\\SelfLearning\\SAI\\DBIIR\\rainbows\\workload.txt");
        params.setProperty("schema.file", "H:\\SelfLearning\\SAI\\DBIIR\\rainbows\\schema.txt");
        params.setProperty("ordered.schema.file", "H:\\SelfLearning\\SAI\\DBIIR\\rainbows\\schema_ordered.txt");
        params.setProperty("seek.cost.function", "power");
        params.setProperty("computation.budget", "100");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
    }
}
