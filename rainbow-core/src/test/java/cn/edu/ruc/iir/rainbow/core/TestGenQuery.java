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
 * @ClassName: TestGenQuery
 * @Description: test gen query
 * @author: Tao
 * @date: Create in 2017-08-13 18:31
 **/
public class TestGenQuery {

    @Test
    public void test() {
        Invoker invoker = InvokerFactory.Instance().getInvoker(INVOKER.GENERATE_QUERY);
        Properties params = new Properties();
        params.setProperty("table.name", "PARQUET_TEST");
        params.setProperty("namenode", "presto00");
        params.setProperty("schema.file", "G:\\DBIIR\\rainbow\\rainbow-core\\src\\test\\java\\dataset\\schema_dupped.txt");
        params.setProperty("workload.file", "G:\\DBIIR\\rainbow\\rainbow-core\\src\\test\\java\\dataset\\workload_dupped.txt");
        params.setProperty("spark.query.file", "G:\\DBIIR\\rainbow\\rainbow-core\\src\\test\\java\\dataset\\spark_dupped.sql");
        params.setProperty("hive.query.file", "G:\\DBIIR\\rainbow\\rainbow-core\\src\\test\\java\\dataset\\hive_dupped.sql");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
    }
}
