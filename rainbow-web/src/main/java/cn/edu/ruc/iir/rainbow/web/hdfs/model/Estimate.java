package cn.edu.ruc.iir.rainbow.web.hdfs.model;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.web.hdfs.model
 * @ClassName: Layout
 * @Description: record for layout strategy
 * @author: Tao
 * @date: Create in 2017-11-05 20:05
 **/
public class Estimate {
    private String no;
    private String id;

    public Estimate() {
    }

    public Estimate(String no, String id) {
        this.no = no;
        this.id = id;
    }

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
