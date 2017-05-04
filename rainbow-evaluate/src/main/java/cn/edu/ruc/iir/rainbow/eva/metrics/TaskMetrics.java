package cn.edu.ruc.iir.rainbow.eva.metrics;

/**
 * Created by hank on 1/28/2015.
 */
public class TaskMetrics {
    private int id;
    private String status;
    private int duration;
    private int schedulerDelay;
    private int taskDeTime;
    private int gcTime;
    private int resSerTime;
    private int getResTime;
    private int writeTime;

    public int getWriteTime() {
        return writeTime;
    }

    public int getId() {
        return id;
    }

    public String getStatus() {
        return status;
    }

    public int getDuration() {
        return duration;
    }

    public int getSchedulerDelay() {
        return schedulerDelay;
    }

    public int getTaskDeTime() {
        return taskDeTime;
    }

    public int getGcTime() {
        return gcTime;
    }

    public int getResSerTime() {
        return resSerTime;
    }

    public int getGetResTime() {
        return getResTime;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public void setSchedulerDelay(int schedulerDelay) {
        this.schedulerDelay = schedulerDelay;
    }

    public void setTaskDeTime(int taskDeTime) {
        this.taskDeTime = taskDeTime;
    }

    public void setGcTime(int gcTime) {
        this.gcTime = gcTime;
    }

    public void setResSerTime(int resSerTime) {
        this.resSerTime = resSerTime;
    }

    public void setGetResTime(int getResTime) {
        this.getResTime = getResTime;
    }

    public void setWriteTime(int writeTime) {
        this.writeTime = writeTime;
    }
}
