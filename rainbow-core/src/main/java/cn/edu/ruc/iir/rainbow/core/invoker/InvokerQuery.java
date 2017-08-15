package cn.edu.ruc.iir.rainbow.core.invoker;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.CommandException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.core.receiver.ReceiverQuery;
import cn.edu.ruc.iir.rainbow.layout.cmd.CmdGenerateQuery;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.core.invoker
 * @ClassName: InvokerQuery
 * @Description: invoker query
 * @author: Tao
 * @date: Create in 2017-08-13 18:17
 **/
public class InvokerQuery extends Invoker
{
    @Override
    protected void createCommands()
    {
        Command command = new CmdGenerateQuery();
        command.setReceiver(new ReceiverQuery());
        try
        {
            this.addCommand(command);
        } catch (CommandException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "error when creating query command", e);
        }
    }
}
