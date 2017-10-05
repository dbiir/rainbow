package cn.edu.ruc.iir.rainbow.web.hdfs.model;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.web.hdfs.model
 * @ClassName: State
 * @Description: Contains the time and the state
 * @author: Tao
 * @date: Create in 2017-09-17 10:38
 **/
public class State {

    private String time;
    private String desc;

    public State() {
    }

    public State(String time, String desc) {
        this.time = time;
        this.desc = desc;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
