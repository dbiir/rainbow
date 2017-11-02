package cn.edu.ruc.iir.rainbow.cli;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.layout.invoker.*;
import cn.edu.ruc.iir.rainbow.redirect.invoker.InvokerBuildIndex;
import cn.edu.ruc.iir.rainbow.redirect.invoker.InvokerRedirect;

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
            case PERFESTIMATION:
                return new InvokerPERFESTIMATION();
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
