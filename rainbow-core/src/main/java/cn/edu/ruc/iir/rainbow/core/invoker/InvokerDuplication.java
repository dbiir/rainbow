package cn.edu.ruc.iir.rainbow.core.invoker;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.CommandException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.core.receiver.ReceiverDuplication;
import cn.edu.ruc.iir.rainbow.layout.cmd.CmdDuplication;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.core.invoker
 * @ClassName: InvokerDuplication
 * @Description: duplication invoker
 * @author: Tao
 * @date: Create in 2017-08-13 10:57
 **/
public class InvokerDuplication extends Invoker
{

    @Override
    protected void createCommands()
    {
        Command command = new CmdDuplication();
        command.setReceiver(new ReceiverDuplication());
        try
        {
            this.addCommand(command);
        } catch (CommandException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "error when creating DUPLICATION command", e);
        }
    }
}
