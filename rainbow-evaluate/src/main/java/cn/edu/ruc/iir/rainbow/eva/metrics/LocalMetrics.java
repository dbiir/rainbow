package cn.edu.ruc.iir.rainbow.eva.metrics;

import cn.edu.ruc.iir.rainbow.eva.domain.Column;

import java.util.List;

/**
 * Created by hank on 2015/2/27.
 */
public class LocalMetrics
{
    private List<Column> columns = null;
    private long readerCount = 0;
    private long rouGroupCount = 0;
    private long rowCount = 0;
    private long timeMillis = 0;

    public LocalMetrics(List<Column> columns, long readerCount, long rouGroupCount, long rowCount, long timeMillis)
    {
        this.columns = columns;
        this.readerCount = readerCount;
        this.rouGroupCount = rouGroupCount;
        this.rowCount = rowCount;
        this.timeMillis = timeMillis;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public void setColumns(List<Column> columns)
    {
        this.columns = columns;
    }

    public long getReaderCount()
    {
        return readerCount;
    }

    public void setReaderCount(long readerCount)
    {
        this.readerCount = readerCount;
    }

    public long getRouGroupCount()
    {
        return rouGroupCount;
    }

    public void setRouGroupCount(long rouGroupCount)
    {
        this.rouGroupCount = rouGroupCount;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public void setRowCount(long rowCount)
    {
        this.rowCount = rowCount;
    }

    public long getTimeMillis()
    {
        return timeMillis;
    }

    public void setTimeMillis(long timeMillis)
    {
        this.timeMillis = timeMillis;
    }
}
