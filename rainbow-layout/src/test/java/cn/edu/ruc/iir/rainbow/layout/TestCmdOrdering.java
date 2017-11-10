package cn.edu.ruc.iir.rainbow.layout;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.layout.cmd.CmdOrdering;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class TestCmdOrdering
{
    @Test
    public void test ()
    {
        Properties params = new Properties();
        params.setProperty("algorithm.name", "scoa.gs");
        params.setProperty("schema.file", TestScoaGS.class.getResource("/schema.txt").getFile());
        params.setProperty("workload.file", TestScoaGS.class.getResource("/workload.txt").getFile());
        params.setProperty("ordered.schema.file", TestScoaGS.class.getResource("/").getFile() + "ordered_schema.txt");
        params.setProperty("seek.cost.function", "power");
        params.setProperty("computation.budget", "100");
        params.setProperty("num.row.group", "100");
        params.setProperty("row.group.size", "134217728");

        Command command = new CmdOrdering();
        command.setReceiver(new Receiver()
        {
            @Override
            public void progress(double percentage)
            {
                System.out.println("ORDERING: " + ((int)(percentage * 10000) / 100.0) + "%    ");
                //System.out.print("\rORDERING: " + ((int)(percentage * 10000) / 100.0) + "%    ");
            }

            @Override
            public void action(Properties results)
            {
                System.out.println(results.getProperty("success"));
                System.out.println(results.getProperty("init.cost"));
                System.out.println(results.getProperty("ordered.schema.file"));
                System.out.println(results.getProperty("final.cost"));
                System.out.println(results.getProperty("row.group.size"));
                System.out.println(results.getProperty("num.row.group"));
                System.out.println("\nFinish.");
            }
        });
        command.execute(params);
    }
}
