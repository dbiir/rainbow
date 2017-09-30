package cn.edu.ruc.iir.rainbow.eva;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.eva.invoker.InvokerWorkloadVectorEvaluation;
import org.junit.Test;

import java.util.Properties;

public class TestWorkloadVectorEvaluation
{
    @Test
    public void test()
    {
        ConfigFactory.Instance().LoadProperties("/home/hank/Desktop/rainbow/rainbow-evaluate/rainbow.properties");
        Properties params = new Properties();
        params.setProperty("method", "SPARK2");
        params.setProperty("format", "PARQUET");
        params.setProperty("table.dirs", "/rainbow/parq_new,/rainbow/parq_ordered");
        params.setProperty("table.names", "parq,parq_ordered");
        params.setProperty("workload.file", "/home/hank/Desktop/rainbow/rainbow-evaluate/workload.txt");
        params.setProperty("log.dir", "/home/hank/Desktop/rainbow/rainbow-evaluate/workload_eva/");
        params.setProperty("drop.cache", "true");
        params.setProperty("drop.caches.sh", "/home/hank/dev/idea-projects/rainbow/rainbow-evaluate/src/test/resources/drop_caches.sh");

        Invoker invoker = new InvokerWorkloadVectorEvaluation();
        try
        {
            invoker.executeCommands(params);
        } catch (InvokerException e)
        {
            e.printStackTrace();
        }
    }
}
