package cn.edu.ruc.iir.rainbow.web.hdfs.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.web.hdfs.model
 * @ClassName: Statistic
 * @Description: for the display
 * @author: Tao
 * @date: Create in 2017-09-23 15:34
 **/
public class Statistic {
    private String name;
    private List<double[]> list = new ArrayList<double[]>();

    public Statistic() {
    }

    public Statistic(String name, List<double[]> list) {
        this.name = name;
        this.list = list;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<double[]> getList() {
        return list;
    }

    public void setList(List<double[]> list) {
        this.list = list;
    }
}
