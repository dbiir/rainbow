package cn.edu.ruc.iir.rainbow.eva;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.eva.invoker.InvokerWorkloadVectorEvaluation;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class TestWorkloadVectorEvaluation
{
    @Test
    public void test()
    {
        ConfigFactory.Instance().LoadProperties("H:\\SelfLearning\\SAI\\DBIIR\\rainbow\\rainbow-common\\src\\main\\resources\\" +
                "rainbow.properties");
        Properties params = new Properties();
        params.setProperty("method", "PRESTO");
        params.setProperty("format", "PARQUET");
        params.setProperty("table.dirs", "/rainbow-web/evaluate/sampling/44ba30d7abdbe13ab2c886f18c0f5555/ordered_2");
        params.setProperty("table.names", "parquet_44ba30d7abdbe13ab2c886f18c0f5555_2");
        params.setProperty("workload.file", "H:\\SelfLearning\\SAI\\DBIIR\\rainbows\\pipeline\\2a37a292b82a7227da22fc6c28e508bd\\workload.txt");
        params.setProperty("log.dir", "H:\\SelfLearning\\SAI\\DBIIR\\rainbows\\pipeline\\2a37a292b82a7227da22fc6c28e508bd\\workload_eva");
        params.setProperty("drop.cache", "false");
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
