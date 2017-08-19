package cn.edu.ruc.iir.rainbow.eva.invoker;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.CommandException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.eva.receiver.ReceiverWorkloadEvaluation;
import cn.edu.ruc.iir.rainbow.eva.cmd.CmdWorkloadEvaluation;

public class InvokerWorkloadEvaluation extends Invoker
{
    /**
     * create this.command and set receiver for it
     */
    @Override
    protected void createCommands()
    {
        // combine command to proper receiver
        Command command = new CmdWorkloadEvaluation();
        command.setReceiver(new ReceiverWorkloadEvaluation());
        try
        {
            this.addCommand(command);
        } catch (CommandException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "error when creating WORKLOAD_EVALUATION command.", e);
        }
    }
}
