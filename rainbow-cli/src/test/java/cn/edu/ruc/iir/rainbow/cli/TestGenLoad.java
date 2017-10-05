package cn.edu.ruc.iir.rainbow.cli;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import org.junit.Test;

import java.util.Properties;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.cli
 * @ClassName: TestGenLoad
 * @Description: Test Ordered Data
 * @author: Tao
 * @date: Create in 2017-08-13 12:10
 **/
public class TestGenLoad
{


    @Test
    public void test() {
        Invoker invoker = InvokerFactory.Instance().getInvoker(INVOKER.GENERATE_DDL);
        Properties params = new Properties();
        params.setProperty("table.name", "PARQUET_DUPPED_TEST");
        params.setProperty("overwrite", "true");
        params.setProperty("schema.file", "G:\\DBIIR\\rainbow\\rainbow-cli\\src\\test\\java\\dataset\\schema_dupped.txt");
        params.setProperty("load.file", "G:\\DBIIR\\rainbow\\rainbow-cli\\src\\test\\java\\dataset\\parquet_dupped_load.sql");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
    }
}
