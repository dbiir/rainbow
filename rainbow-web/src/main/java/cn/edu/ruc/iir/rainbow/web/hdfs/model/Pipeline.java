package cn.edu.ruc.iir.rainbow.web.hdfs.model;

import java.util.List;

/**
 * @ClassName: Pipeline
 * @Title:
 * @Description: info of pipline
 * @param:
 * @author: Tao
 * @date: 23:31 2017/9/12
 */
public class Pipeline
{

    private String no;
    private String caption;
    private long lifeTime;
    private double threshold;

    private String format;

    private Integer columnOrder;
    private Integer rowGroupSize;
    private String dataSource;

    private String compression;
    private String url;
    private String storePath;
    private String description;

    private int state; // 0:remove   1: stop    2: remove      3: accept
    private int id;

    public Pipeline()
    {
        super();
    }

    public Pipeline(List<String> names)
    {
        this.no = names.get(0);
        this.caption = names.get(1);
        this.lifeTime = Long.valueOf(names.get(2));
        this.threshold = Double.valueOf(names.get(3));
        this.format = names.get(4).toUpperCase();
        this.columnOrder = 0;
        this.rowGroupSize = Integer.valueOf(names.get(6));
        this.compression = names.get(7);
        this.dataSource = names.get(8);
        this.url = names.get(9);
        this.storePath = names.get(10);
        this.description = names.get(11);
        this.state = 0;
        this.id = 0;
    }

    public long getLifeTime() {
        return lifeTime;
    }

    public void setLifeTime(long lifeTime) {
        this.lifeTime = lifeTime;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public int getId()
    {
        return id;
    }

    public void setId(int id)
    {
        this.id = id;
    }

    public int getState()
    {
        return state;
    }

    public void setState(int state)
    {
        this.state = state;
    }

    public String getDataSource()
    {
        return dataSource;
    }

    public void setDataSource(String dataSource)
    {
        this.dataSource = dataSource;
    }

    public String getNo()
    {
        return no;
    }

    public void setNo(String no)
    {
        this.no = no;
    }

    public String getCaption()
    {
        return caption;
    }

    public void setCaption(String caption)
    {
        this.caption = caption;
    }

    public String getFormat()
    {
        return format;
    }

    public void setFormat(String format)
    {
        this.format = format;
    }

    public Integer getColumnOrder()
    {
        return columnOrder;
    }

    public void setColumnOrder(Integer columnOrder)
    {
        this.columnOrder = columnOrder;
    }

    public Integer getRowGroupSize()
    {
        return rowGroupSize;
    }

    public void setRowGroupSize(Integer rowGroupSize)
    {
        this.rowGroupSize = rowGroupSize;
    }

    public String getCompression()
    {
        return compression;
    }

    public void setCompression(String compression)
    {
        this.compression = compression;
    }

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public String getStorePath()
    {
        return storePath;
    }

    public void setStorePath(String storePath)
    {
        this.storePath = storePath;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }
}