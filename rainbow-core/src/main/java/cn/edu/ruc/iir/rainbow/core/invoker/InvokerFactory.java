package cn.edu.ruc.iir.rainbow.core.invoker;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;

public class InvokerFactory
{
    private static InvokerFactory instance = new InvokerFactory();

    public static InvokerFactory Instance ()
    {
        return instance;
    }

    private InvokerFactory () { }

    public Invoker getInvoker (INVOKER invokerName)
    {
        switch (invokerName)
        {
            case GENERATE_DDL:
                return new InvokerGenerateDDL();
            // TODO: add more case branch to deal with other INVOKERs...
            default:
                return null;
        }
    }
}
