package cn.edu.ruc.iir.rainbow.redirect.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.redirect.domain.AccessPattern;
import cn.edu.ruc.iir.rainbow.redirect.domain.ColumnSet;
import cn.edu.ruc.iir.rainbow.redirect.index.Index;
import cn.edu.ruc.iir.rainbow.redirect.index.IndexFactory;

import java.util.Properties;

public class CmdRedirect implements Command
{
    private Receiver receiver = null;
    @Override
    public void setReceiver(Receiver receiver)
    {
        this.receiver = receiver;
    }

    /**
     * This method redirects the columns accessed by a query to the proper column replicas.
     *
     * params should contain the following settings:
     * <ol>
     *   <li>query.id, id of the query</li>
     *   <li>column.set, the set of origin column (not a column replica) been accessed by the query,
     *   separated by comma</li>
     * </ol>
     *
     * this method will pass the following results to receiver:
     * <ol>
     *   <li>success, true or false</li>
     *   <li>access.pattern, the redirected access pattern (set of column replica names) for the query,
     *   separated by comma</li>
     * </ol>
     * @param params
     */
    @Override
    public void execute(Properties params)
    {
        String[] columns = params.getProperty("column.set").split(",");
        ColumnSet columnSet = new ColumnSet();
        for (String column : columns)
        {
            columnSet.addColumn(column);
        }
        Index index = IndexFactory.Instance().getIndex(
                ConfigFactory.Instance().getProperty("inverted.index.name"));
        AccessPattern bestPattern = index.search(columnSet);

        Properties results = new Properties(params);
        results.setProperty("success", "true");
        results.setProperty("access.pattern", bestPattern.toString());

        if (receiver != null)
        {
            receiver.progress(1.0);
            receiver.action(results);
        }
    }
}
