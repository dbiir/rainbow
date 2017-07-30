package cn.edu.ruc.iir.rainbow.redirect.domain;

import cn.edu.ruc.iir.rainbow.common.exception.ColumnOrderException;

import java.util.*;

public class AccessPattern
{
    private List<String> pattern = null;
    private Map<String, String> columnToReplicaMap = null;

    public AccessPattern ()
    {
        this.pattern = new ArrayList<>();
        this.columnToReplicaMap = new HashMap<>();
    }

    public AccessPattern (List<String> pattern)
    {
        this();
        for (String columnReplica : this.pattern)
        {
            this.addColumnReplica(columnReplica);
        }
    }

    public void addColumnReplica (String columnReplica)
    {
        this.pattern.add(columnReplica);
        int i = columnReplica.lastIndexOf('_');
        String column = columnReplica.substring(0, i > 0 ? i :columnReplica.length());
        this.columnToReplicaMap.put(column, columnReplica);
    }

    public String getColumnReplica (String column)
    {
        return this.columnToReplicaMap.get(column);
    }

    public AccessPattern generatePattern (ColumnSet columnSet, List<String> columnOrder) throws ColumnOrderException
    {
        AccessPattern result = new AccessPattern();
        Set<String> replicaSet = new HashSet<>(this.pattern);
        for (String columnReplica : columnOrder)
        {
            String column = columnReplica.substring(0, columnReplica.lastIndexOf('_'));
            if (columnSet.contains(column))
            {
                // this column is accessed by the query.
                if (replicaSet.contains(columnReplica)
                        || (!this.columnToReplicaMap.containsKey(column)))
                {
                    // this column replica is the right one for the query to access.
                    result.addColumnReplica(columnReplica);
                }
            }
        }
        if (result.pattern.size() != columnSet.size())
        {
            throw new ColumnOrderException("some column replicas may be missing in the column order.");
        }
        return result;
    }
}
