package cn.edu.ruc.iir.rainbow.workload.cli;

import cn.edu.ruc.iir.rainbow.workload.cmd.CmdWorkloadEva;
import cn.edu.ruc.iir.rainbow.workload.util.Settings;

import java.util.Properties;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.workload.cli
 * @ClassName: Client
 * @Description: To issue sql to presto
 * @author: taoyouxian
 * @date: Create in 2018-01-16 9:04
 **/
public class Client {

    public static void main(String[] args) {

        Settings.APC_TASK = true;
        String tableName = "text";
        Properties params = new Properties();
        params.setProperty("format", "PARQUET");
        params.setProperty("table.dirs", "");
        params.setProperty("table.names", tableName);
        params.setProperty("workload.file", Settings.WORKLOAD_PATH);
        params.setProperty("drop.cache", "false");
        params.setProperty("drop.caches.sh", "/home/tao/software/station/DBIIR/rainbow/rainbow-evaluate/src/test/resources/drop_caches.sh");
        CmdWorkloadEva cmdWorkloadEva = new CmdWorkloadEva();
        cmdWorkloadEva.execute(params);
    }
}
