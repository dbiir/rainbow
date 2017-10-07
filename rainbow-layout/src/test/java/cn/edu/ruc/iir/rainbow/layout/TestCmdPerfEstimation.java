package cn.edu.ruc.iir.rainbow.layout;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.layout.cmd.CmdPerfEstimation;
import org.junit.Test;

import java.util.Properties;

public class TestCmdPerfEstimation
{
    @Test
    public void test ()
    {
        Properties params = new Properties();
        params.setProperty("schema.file", TestScoaGS.class.getResource("/ordered_schema.txt").getFile());
        params.setProperty("workload.file", TestScoaGS.class.getResource("/workload.txt").getFile());
        params.setProperty("log.file", TestScoaGS.class.getResource("/").getFile() + "ordered_estimate_duration.csv");
        params.setProperty("seek.cost.function", "power");
        params.setProperty("num.row.group", "13");

        Command command = new CmdPerfEstimation();
        command.setReceiver(new Receiver()
        {
            @Override
            public void progress(double percentage)
            {
                System.out.println(percentage);
            }

            @Override
            public void action(Properties results)
            {
                System.out.println("Finish.");
            }
        });

        command.execute(params);
    }
}
