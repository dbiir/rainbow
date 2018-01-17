package cn.edu.ruc.iir.rainbow.workload.util;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.xspace.workload.util
 * @ClassName: Settings
 * @Description: to store the info needed by http or functions
 * @author: Tao
 * @date: Create in 2017-09-30 8:58
 **/
public class Settings {

    public static final String PRESTO_QUERY = "http://10.77.40.236:8080/v1/query";
    public static final String WORKLOAD_PATH = "/home/tao/software/station/Workplace/workload.txt";
    public static final long WORKLOAD_EVA = 10 * 1000;

    public static boolean APC_TASK = false;
    public static final String APC_PATH = "/home/tao/software/station/Workplace/APC.txt";

}
