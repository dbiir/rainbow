package cn.edu.ruc.iir.rainbow.workload;

import java.util.HashSet;
import java.util.Set;

public class AccessPattern implements Comparable<AccessPattern>
{
    private String queryId;
    private Set<String> columns = null;
    private double weight = 1;
    private long timeStamp = 0;


    public AccessPattern(String queryId)
    {
        this.queryId = queryId;
        this.columns = new HashSet<>();
    }

    public AccessPattern(String queryId, double weight)
    {
        this.queryId = queryId;
        this.weight = weight;
        this.columns = new HashSet<>();
    }

    public Set<String> getColumns()
    {
        return columns;
    }

    public void setColumns(Set<String> columns)
    {
        this.columns = columns;
    }

    public void addColumn(String column)
    {
        this.columns.add(column);
    }

    public double getWeight()
    {
        return weight;
    }

    public void incrementWeight ()
    {
        this.weight ++;
    }

    public long getTimeStamp()
    {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp)
    {
        this.timeStamp = timeStamp;
    }

    public String getQueryId()
    {
        return queryId;
    }

    @Override
    public int hashCode()
    {
        // hash code of a set of the sum of the hash codes of its elements.
        return this.columns.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        boolean ret = true;
        if (o instanceof AccessPattern)
        {
            AccessPattern pattern = (AccessPattern) o;
            if (pattern.columns.size() == this.columns.size())
            {
                for (String column : pattern.columns)
                {
                    if (!this.columns.contains(column))
                    {
                        ret = false;
                        break;
                    }
                }
            }
            else
            {
                ret = false;
            }
        }
        else
        {
            ret = false;
        }
        return ret;
    }

    @Override
    protected Object clone()
    {
        AccessPattern ret = new AccessPattern(this.queryId, this.weight);
        ret.columns.addAll(this.columns);
        return ret;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder(this.queryId);
        builder.append("\t").append(weight).append("\t");
        for (String column : this.columns)
        {
            builder.append(column).append(",");
        }
        return builder.substring(0, builder.length()-1);
    }

    @Override
    public int compareTo(AccessPattern accessPattern)
    {
        if (this.timeStamp < accessPattern.timeStamp)
        {
            return -1;
        }
        else if (this.timeStamp > accessPattern.timeStamp)
        {
            return 1;
        }
        return this.hashCode() - accessPattern.hashCode();
    }
}
