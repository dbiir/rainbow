package cn.edu.ruc.iir.rainbow.common.cmd;

import cn.edu.ruc.iir.rainbow.common.exception.CommandException;

import java.util.Properties;

/**
 * Created by hank on 17-5-4.
 */
abstract public class Invoker
{
    private Command command = null;

    public Invoker ()
    {
        this.createCommand();
    }

    /**
     * create this.command and set receiver for it
     */
    abstract protected void createCommand ();

    final protected void setCommand(Command command) throws CommandException
    {
        if (command != null)
        {
            this.command = command;
        }
        else
        {
            throw new CommandException("command is null.");
        }
    }

    public final void executeCommand (Properties params) throws CommandException
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
