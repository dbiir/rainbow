package cn.edu.ruc.iir.rainbow.web.hdfs.model;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.web.hdfs.model
 * @ClassName: Layout
 * @Description: record for layout strategy
 * @author: Tao
 * @date: Create in 2017-09-27 20:36
 **/
public class Layout {
    private String no;
    private String format;
    private Integer columnOrder;
    private Integer rowGroupSize;
    private String dataSource;
    private String compression;

    private String state;
    private String time;

    public Layout() {
    }

    public Layout(String no, String format, Integer columnOrder, Integer rowGroupSize, String dataSource, String compression) {
        this.no = no;
        this.format = format;
        this.columnOrder = columnOrder;
        this.rowGroupSize = rowGroupSize;
        this.dataSource = dataSource;
        this.compression = compression;
    }

    public Layout(Pipeline pipline, String state, String time) {
        this.no = pipline.getNo();
        this.format = pipline.getFormat();
        this.columnOrder = pipline.getColumnOrder();
        this.rowGroupSize = pipline.getRowGroupSize();
        this.dataSource = pipline.getDataSource();
        this.compression = pipline.getCompression();
        this.state = state;
        this.time = time;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Integer getColumnOrder() {
        return columnOrder;
    }

    public void setColumnOrder(Integer columnOrder) {
        this.columnOrder = columnOrder;
    }

    public Integer getRowGroupSize() {
        return rowGroupSize;
    }

    public void setRowGroupSize(Integer rowGroupSize) {
        this.rowGroupSize = rowGroupSize;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getCompression() {
        return compression;
    }

    public void setCompression(String compression) {
        this.compression = compression;
    }
}
