package cn.edu.ruc.iir.rainbow.core;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import cn.edu.ruc.iir.rainbow.core.invoker.INVOKER;
import cn.edu.ruc.iir.rainbow.core.invoker.InvokerFactory;
import org.junit.Test;

import java.util.Properties;

public class TestGenDDL
{
    @Test
    public void test ()
    {
        Invoker invoker = InvokerFactory.Instance().getInvoker(INVOKER.GENERATE_SQL);
        Properties params = new Properties();
        params.setProperty("file.format", "ORC");
        params.setProperty("table.name", "orc_test");
        params.setProperty("overwrite", "true");
        params.setProperty("schema.file", "/home/hank/dev/idea-projects/rainbow/rainbow-redirect/src/test/resources/schema.txt");
        params.setProperty("ddl.file", "/home/hank/dev/idea-projects/rainbow/rainbow-redirect/src/test/resources/ddl.txt");
        params.setProperty("load.file", "/home/hank/dev/idea-projects/rainbow/rainbow-redirect/src/test/resources/load.txt");
        try
        {
            invoker.executeCommands(params);
        } catch (InvokerException e)
        {
            e.printStackTrace();
        }
    }
}
