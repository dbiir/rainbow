package cn.edu.ruc.iir.rainbow.redirect.domain;

import cn.edu.ruc.iir.rainbow.common.exception.ColumnOrderException;

import java.util.*;

public class AccessPattern
{
    // it seems that this.pattern can be a Set.
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

    public int size ()
    {
        return this.pattern.size();
    }

    public ColumnSet getColumnSet ()
    {
        return new ColumnSet(this.columnToReplicaMap.keySet());
    }

    public boolean contaiansColumn (String column)
    {
        return this.columnToReplicaMap.containsKey(column);
    }

    public AccessPattern generatePattern (ColumnSet columnSet, List<String> columnOrder) throws ColumnOrderException
    {
        AccessPattern result = new AccessPattern();
        Set<String> replicaSet = new HashSet<>(this.pattern);
        for (String columnReplica : columnOrder)
        {
            // iterate through the column order.
            int i = columnReplica.lastIndexOf('_');
            String column = columnReplica.substring(0, i > 0 ? i :columnReplica.length());
            if (columnSet.contains(column))
            {
                // this column is accessed by the query.
                if (replicaSet.contains(columnReplica)
                        || (!this.columnToReplicaMap.containsKey(column)))
                {
                    // this column replica is the right one for the query to access.
                    // this happens in two cases:
                    // 1. this column replica is accessed in this pattern;
                    // 2. the origin column of this column replica is not accessed in this pattern.
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

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        for (String columnReplica : this.pattern)
        {
            builder.append(",").append(columnReplica);
        }
        return builder.substring(1);
    }
}
