package cn.edu.ruc.iir.rainbow.core.invoker;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;

public class InvokerFactory
{
    private static InvokerFactory instance = new InvokerFactory();

    public static InvokerFactory Instance()
    {
        return instance;
    }

    private InvokerFactory()
    {
    }

    public Invoker getInvoker(INVOKER invoker)
    {
        switch (invoker)
        {
            case GET_COLUMN_SIZE:
                return new InvokerGetColumnSize();
            case ORDERING:
                return new InvokerOrdering();
            case DUPLICATION:
                return new InvokerDuplication();
            case GENERATE_DDL:
                return new InvokerGenerateDDL();
            case GENERATE_LOAD:
                return new InvokerGenerateLoad();
            case GENERATE_QUERY:
                return new InvokerGenerateQuery();
            case BUILD_INDEX:
                return new InvokerBuildIndex();
            case REDIRECT:
                return new InvokerRedirect();
            default:
                return null;
        }
    }

    public Invoker getInvoker(String invokerName)
    {
        INVOKER invoker = INVOKER.valueOf(invokerName);
        return getInvoker(invoker);
    }
}
