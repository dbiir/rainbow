package cn.edu.ruc.iir.rainbow.redirect.domain;

import java.util.List;
import java.util.Set;

public class Query
{
    private ColumnSet columnSet = null;
    private AccessPattern accessPattern = null;

    public Query (ColumnSet columnSet, AccessPattern accessPattern)
    {
        this.columnSet = columnSet;
        this.accessPattern = accessPattern;
    }

    public Query (Set<String> columnSet, List<String> accessPattern)
    {
        this (new ColumnSet(columnSet), new AccessPattern(accessPattern));
    }

    public ColumnSet getColumnSet()
    {
        return columnSet;
    }

    public AccessPattern getAccessPattern()
    {
        return accessPattern;
    }

    public void setColumnSet(ColumnSet columnSet)
    {
        this.columnSet = columnSet;
    }

    public void setAccessPattern(AccessPattern accessPattern)
    {
        this.accessPattern = accessPattern;
    }
}
