package cn.edu.ruc.iir.rainbow.eva.metrics;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by hank on 2015/1/28.
 */
public class TaskMetricsColumnId {
    /**
     * This class is the mapping between column id in the html of Spark web ui and Spark task metrics.
     */
    private TaskMetricsColumnId() {
    }

    private static Map<String, Integer> map = new HashMap<String, Integer>();

    static {
        map.put("id", 1);
        map.put("status", 3);
        map.put("duration", 7);
        //...add more metrics to column id mappings.
    }

    public static int getColumnId (String metricsName)
    {
        return map.get(metricsName);
    }
}
