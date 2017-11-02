package cn.edu.ruc.iir.rainbow.layout.invoker;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.CommandException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.layout.cmd.CmdPerfEstimation;
import cn.edu.ruc.iir.rainbow.layout.receiver.ReceiverPERFESTIMATION;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.cli.invoker
 * @ClassName: InvokerOrdering
 * @Description: ordering invoker
 * @author: Tao
 * @date: Create in 2017-08-13 10:56
 **/
public class InvokerPERFESTIMATION extends Invoker
{

    @Override
    protected void createCommands()
    {
        Command command = new CmdPerfEstimation();
        command.setReceiver(new ReceiverPERFESTIMATION());
        try
        {
            this.addCommand(command);
        } catch (CommandException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "error when creating PERFESTIMATION command", e);
        }
    }
}
