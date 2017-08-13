package cn.edu.ruc.iir.rainbow.redirect;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.redirect.cmd.CmdBuildIndex;
import cn.edu.ruc.iir.rainbow.redirect.cmd.CmdRedirect;
import org.junit.Test;

import java.util.Properties;

public class TestRedirect
{
    @Test
    public void test ()
    {
        Properties params = new Properties();
        params.setProperty("dupped.schema.file", this.getClass().getResource("/schema_dupped.txt").getFile());
        params.setProperty("dupped.workload.file", this.getClass().getResource("/workload_dupped.txt").getFile());
        params.setProperty("query.id", "1");
        params.setProperty("column.set", "Column_311,Column_361,Column_1000,Column_256,Column_111");

        Command command = new CmdBuildIndex();
        command.execute(params);

        Receiver receiver = new Receiver()
        {
            @Override
            public void progress(double percentage)
            {
                System.out.println(percentage);
            }

            @Override
            public void action(Properties results)
            {
                System.out.println(results.getProperty("access.pattern"));
            }
        };

        Command command1 = new CmdRedirect();
        command1.setReceiver(receiver);
        command1.execute(params);
    }
}
