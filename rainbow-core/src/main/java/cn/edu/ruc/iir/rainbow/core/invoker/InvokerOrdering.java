package cn.edu.ruc.iir.rainbow.core.invoker;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.CommandException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.core.receiver.ReceiverOrdering;
import cn.edu.ruc.iir.rainbow.layout.cmd.CmdOrdering;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.core.invoker
 * @ClassName: InvokerOrdering
 * @Description: ordering invoker
 * @author: Tao
 * @date: Create in 2017-08-13 10:56
 **/
public class InvokerOrdering extends Invoker {

    @Override
    protected void createCommands() {
        Command command = new CmdOrdering();
        command.setReceiver(new ReceiverOrdering());
        try {
            this.addCommand(command);
        } catch (CommandException e) {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "error when creating ordering command", e);
        }
    }
}
