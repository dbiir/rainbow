package cn.edu.ruc.iir.rainbow.layout;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.layout.cmd.CmdDuplication;
import org.junit.Test;

import java.util.Properties;

public class TestCmdDuplication
{
    @Test
    public void test ()
    {
        Properties params = new Properties();
        params.setProperty("algorithm.name", "insertion");
        params.setProperty("schema.file", TestCmdDuplication.class.getResource("/schema.txt").getFile());
        params.setProperty("workload.file", TestCmdDuplication.class.getResource("/workload.txt").getFile());
        params.setProperty("dupped.schema.file", TestCmdDuplication.class.getResource("/").getFile() + "dupped_schema.txt");
        params.setProperty("dupped.workload.file", TestCmdDuplication.class.getResource("/").getFile() + "dupped_workload.txt");
        params.setProperty("seek.cost.function", "power");
        params.setProperty("computation.budget", "1000");

        Command command = new CmdDuplication();
        command.execute(params);
    }
}
