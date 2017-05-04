package cn.edu.ruc.iir.rainbow.common.cmd;

import cn.edu.ruc.iir.rainbow.common.exception.CommandException;

/**
 * Created by hank on 17-5-4.
 */
public class Invoker
{
    private Command command = null;

    public void setCommand(Command command, Receiver receiver) throws CommandException
    {
        this.command = command;
        if (command != null && receiver != null)
        {
            command.setReceiver(receiver);
        }
        else if (command == null)
        {
            throw new CommandException("command is null.");
        }
        else
        {
            throw new CommandException("receiver is null.");
        }
    }

    public void executeCommand (String[] params) throws CommandException
    {
        if (this.command != null)
        {
            this.command.execute(params);
        }
        else
        {
            throw new CommandException("command is null.");
        }
    }
}
