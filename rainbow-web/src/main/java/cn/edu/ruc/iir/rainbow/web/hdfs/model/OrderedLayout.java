package cn.edu.ruc.iir.rainbow.web.hdfs.model;

import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.web.hdfs.model
 * @ClassName: OrderedLayout
 * @Description: Describe ordered layout
 * @author: taoyouxian
 * @date: Create in 2017-11-02 14:02
 **/
public class OrderedLayout {

    private String no;
    private List<Layout> layouts;
    private int count;

    public OrderedLayout() {
    }

    public OrderedLayout(String no, List<Layout> layouts, int count) {
        this.no = no;
        this.layouts = layouts;
        this.count = count;
    }

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    public List<Layout> getLayouts() {
        return layouts;
    }

    public void setLayouts(List<Layout> layouts) {
        this.layouts = layouts;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
