package cn.edu.ruc.iir.rainbow.core.invoker;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;

public class InvokerFactory {
    private static InvokerFactory instance = new InvokerFactory();

    public static InvokerFactory Instance() {
        return instance;
    }

    private InvokerFactory() {
    }

    public Invoker getInvoker(INVOKER invokerName) {
        switch (invokerName) {
            case GENERATE_SQL:
                return new InvokerGenerateSQL();
            // TODO: add more case branch to deal with other INVOKERs...
            case ORDERING:
                return new InvokerOrdering();
            case DUPLICATION:
                return new InvokerDuplication();
            case QUERY:
                return new InvokerQuery();
            default:
                return null;
        }
    }
}
