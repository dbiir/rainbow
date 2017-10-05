package cn.edu.ruc.iir.rainbow.cli;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import org.junit.Test;

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
        params.setProperty("algorithm.name", "scoa");
        params.setProperty("workload.file", "G:\\DBIIR\\rainbow\\rainbow-benchmark\\dataset\\workload.txt");
        params.setProperty("schema.file", "G:\\DBIIR\\rainbow\\rainbow-benchmark\\dataset\\schema.txt");
        params.setProperty("ordered.schema.file", "G:\\DBIIR\\rainbow\\rainbow-cli\\src\\test\\java\\dataset\\schema_ordered.txt");
        params.setProperty("seek.cost.function", "power");
        params.setProperty("computation.budget", "200");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
    }
}
