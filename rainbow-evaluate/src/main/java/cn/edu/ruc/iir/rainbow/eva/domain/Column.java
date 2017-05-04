package cn.edu.ruc.iir.rainbow.eva.domain;

import parquet.column.ColumnDescriptor;

/**
 * Created by hank on 2015/2/5.
 */
public class Column implements Comparable<Column>
{
    private int index;
    private String name;
    private ColumnDescriptor descriptor;

    public ColumnDescriptor getDescriptor()
    {
        return descriptor;
    }

    public void setDescriptor(ColumnDescriptor descriptor)
    {
        this.descriptor = descriptor;
    }

    public int getIndex()
    {
        return index;
    }

    public void setIndex(int index)
    {
        this.index = index;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    @Override
    public boolean equals(Object obj)
    {
        Column c = (Column) obj;
        return this.index == c.index;
    }

    @Override
    public int compareTo(Column o)
    {
        if (this.index < o.index)
        {
            return 1;
        }
        else if (this.index > o.index)
        {
            return -1;
        }
        return 0;
    }
}
