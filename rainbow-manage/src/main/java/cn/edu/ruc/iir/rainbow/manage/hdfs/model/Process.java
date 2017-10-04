package cn.edu.ruc.iir.rainbow.manage.hdfs.model;


import java.util.ArrayList;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.manage.hdfs.model
 * @ClassName: Process
 * @Description: To record the process of the pipline
 * @author: Tao
 * @date: Create in 2017-09-17 10:37
 **/
public class Process {

    private String pipelineNo;
    private List<State> pipelineState = new ArrayList<>();

    public Process() {
    }

    public Process(String pipelineNo, List<State> pipelineState) {
        this.pipelineNo = pipelineNo;
        this.pipelineState = pipelineState;
    }

    public String getPipelineNo() {
        return pipelineNo;
    }

    public void setPipelineNo(String pipelineNo) {
        this.pipelineNo = pipelineNo;
    }

    public List<State> getPipelineState() {
        return pipelineState;
    }

    public void setPipelineState(List<State> pipelineState) {
        this.pipelineState = pipelineState;
    }
}
