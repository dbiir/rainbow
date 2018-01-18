package cn.edu.ruc.iir.rainbow.workload.cmd;

import cn.edu.ruc.iir.rainbow.benchmark.util.DateUtil;
import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.workload.eva.PrestoEvaluator;
import cn.edu.ruc.iir.rainbow.workload.util.Settings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Date;
import java.util.Properties;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.workload.cmd
 * @ClassName: CmdWorkloadEva
 * @Description: Cmd to evaluate workload
 * @author: tao
 * @date: Create in 2018-01-16 19:37
 **/
public class CmdWorkloadEva implements Command {

    private Receiver receiver = null;

    @Override
    public void setReceiver(Receiver receiver) {
        this.receiver = receiver;
    }

    @Override
    public void execute(Properties params) {
        try (BufferedReader workloadReader = new BufferedReader(new FileReader(Settings.WORKLOAD_PATH));) {
            boolean dropCache = Boolean.parseBoolean(params.getProperty("drop.cache"));
            String dropCachesSh = params.getProperty("drop.caches.sh");
            String tableName = params.getProperty("table.names");

            // begin evaluate
            String line;
            int i = 0;
            while ((line = workloadReader.readLine()) != null) {
                String columns = line.split("\t")[2];
                String queryId = line.split("\t")[0];
                // get the smallest column as the order by column
                String orderByColumn = null;

                // evaluate
                // clear the caches and buffers
                if (dropCache) {
                    Runtime.getRuntime().exec(dropCachesSh);
                }
                PrestoEvaluator instance = PrestoEvaluator.Instance();
                instance.execute(tableName, columns, orderByColumn);
                i++;
                if (i % 300 == 0 || i % 1000 == 0) {
                    System.out.print(DateUtil.formatTime(new Date()) + "\t");
                    System.out.println("line: " + i);
                }
            }
        } catch (Exception e) {
            System.out.println("CmdWorkloadEva Error. \n" + e.getMessage());
        }
    }
}
