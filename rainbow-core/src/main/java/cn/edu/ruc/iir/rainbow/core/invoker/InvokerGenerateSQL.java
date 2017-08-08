package cn.edu.ruc.iir.rainbow.core.invoker;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.CommandException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.core.receiver.ReceiverGenerateDDL;
import cn.edu.ruc.iir.rainbow.layout.cmd.CmdGenerateDDL;

public class InvokerGenerateSQL extends Invoker
{
    /**
     * create this.command and set receiver for it
     */
    @Override
    protected void createCommands()
    {
        // combine command to proper receiver
        Command command = new CmdGenerateDDL();
        command.setReceiver(new ReceiverGenerateDDL());

        try
        {
            this.addCommand(command);
        } catch (CommandException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "error when creating generateDDL command.", e);
        }
    }
}
