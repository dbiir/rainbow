package cn.edu.ruc.iir.rainbow.core;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import cn.edu.ruc.iir.rainbow.core.invoker.INVOKER;
import cn.edu.ruc.iir.rainbow.core.invoker.InvokerFactory;
import org.junit.Test;

import java.util.Properties;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.core
 * @ClassName: TestDuplication
 * @Description: test duplication
 * @author: Tao
 * @date: Create in 2017-08-13 12:28
 **/
public class TestDuplication {

    @Test
    public void test() {
        Invoker invoker = InvokerFactory.Instance().getInvoker(INVOKER.DUPLICATION);
        Properties params = new Properties();
        params.setProperty("algorithm.name", "insertion");
        params.setProperty("schema.file", "G:\\DBIIR\\rainbow\\rainbow-benchmark\\dataset\\schema.txt");
        params.setProperty("workload.file", "G:\\DBIIR\\rainbow\\rainbow-benchmark\\dataset\\workload.txt");
        params.setProperty("dupped.schema.file", "G:\\DBIIR\\rainbow\\rainbow-core\\src\\test\\java\\dataset\\schema_dupped.txt");
        params.setProperty("dupped.workload.file", "G:\\DBIIR\\rainbow\\rainbow-core\\src\\test\\java\\dataset\\workload_dupped.txt");
        params.setProperty("seek.cost.function", "power");
        params.setProperty("seek.cost.file", "");
        params.setProperty("computation.budget", "200");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
    }
}
