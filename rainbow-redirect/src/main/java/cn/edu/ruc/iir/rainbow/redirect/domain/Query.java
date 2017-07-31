package cn.edu.ruc.iir.rainbow.redirect.domain;

import java.util.List;
import java.util.Set;

public class Query
{
    private String uuid = null;
    private ColumnSet columnSet = null;
    private AccessPattern accessPattern = null;

    public Query (String uuid, ColumnSet columnSet, AccessPattern accessPattern)
    {
        this.uuid = uuid;
        this.columnSet = columnSet;
        this.accessPattern = accessPattern;
    }

    public Query (String uuid, Set<String> columnSet, List<String> accessPattern)
    {
        this (uuid, new ColumnSet(columnSet), new AccessPattern(accessPattern));
    }

    public String getUuid()
    {
        return uuid;
    }

    public ColumnSet getColumnSet()
    {
        return columnSet;
    }

    public AccessPattern getAccessPattern()
    {
        return accessPattern;
    }

    public void setUuid(String uuid)
    {
        this.uuid = uuid;
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
