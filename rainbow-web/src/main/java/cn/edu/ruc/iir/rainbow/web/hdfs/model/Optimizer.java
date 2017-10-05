package cn.edu.ruc.iir.rainbow.web.hdfs.model;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.web.hdfs.model
 * @ClassName: Optimizer
 * @Description: contains ordering, row group, compression
 * @author: Tao
 * @date: Create in 2017-09-15 10:45
 **/
public class Optimizer {

    private Integer columncorder;
    private Integer rowgroupsize;
    private String compression;

    public Optimizer() {
    }

    public Optimizer(Integer columncorder, Integer rowgroupsize, String compression) {
        this.columncorder = columncorder;
        this.rowgroupsize = rowgroupsize;
        this.compression = compression;
    }

    public Integer getColumncorder() {
        return columncorder;
    }

    public void setColumncorder(Integer columncorder) {
        this.columncorder = columncorder;
    }

    public Integer getRowgroupsize() {
        return rowgroupsize;
    }

    public void setRowgroupsize(Integer rowgroupsize) {
        this.rowgroupsize = rowgroupsize;
    }

    public String getCompression() {
        return compression;
    }

    public void setCompression(String compression) {
        this.compression = compression;
    }
}
