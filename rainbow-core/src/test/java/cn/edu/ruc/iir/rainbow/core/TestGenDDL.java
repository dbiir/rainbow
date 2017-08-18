package cn.edu.ruc.iir.rainbow.core;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import cn.edu.ruc.iir.rainbow.core.invoker.INVOKER;
import cn.edu.ruc.iir.rainbow.core.invoker.InvokerFactory;
import org.junit.Test;

import java.util.Properties;

public class TestGenDDL {
    @Test
    public void test() {
        Invoker invoker = InvokerFactory.Instance().getInvoker(INVOKER.GENERATE_DDL);
        Properties params = new Properties();
        params.setProperty("file.format", "PARQUET");
        params.setProperty("table.name", "parquet_test");
        params.setProperty("schema.file", "G:\\DBIIR\\rainbow\\rainbow-benchmark\\dataset\\schema.txt");
        params.setProperty("ddl.file", "G:\\DBIIR\\rainbow\\rainbow-core\\src\\test\\java\\dataset\\parquet_ddl.sql");
        try {
            invoker.executeCommands(params);

            params.setProperty("file.format", "TEXT");
            params.setProperty("ddl.file", "G:\\DBIIR\\rainbow\\rainbow-core\\src\\test\\java\\dataset\\text_ddl.sql");
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
    }
}
