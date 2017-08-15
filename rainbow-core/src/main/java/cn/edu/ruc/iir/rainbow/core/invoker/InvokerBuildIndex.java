package cn.edu.ruc.iir.rainbow.core.invoker;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.CommandException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.core.receiver.ReceiverBuildIndex;
import cn.edu.ruc.iir.rainbow.redirect.cmd.CmdBuildIndex;

public class InvokerBuildIndex extends Invoker
{
    /**
     * create this.command and set receiver for it
     */
    @Override
    protected void createCommands()
    {
        // combine command to proper receiver
        Command command = new CmdBuildIndex();
        command.setReceiver(new ReceiverBuildIndex());
        try
        {
            this.addCommand(command);
        } catch (CommandException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "error when creating BUILD_INDEX command.", e);
        }
    }
}
