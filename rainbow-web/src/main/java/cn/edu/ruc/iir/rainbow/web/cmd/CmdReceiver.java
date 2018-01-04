package cn.edu.ruc.iir.rainbow.web.cmd;

import cn.edu.ruc.iir.rainbow.benchmark.util.DateUtil;
import cn.edu.ruc.iir.rainbow.cli.INVOKER;
import cn.edu.ruc.iir.rainbow.cli.InvokerFactory;
import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.workload.APCFactory;
import cn.edu.ruc.iir.rainbow.workload.AccessPattern;
import cn.edu.ruc.iir.rainbow.workload.AccessPatternCache;
import cn.edu.ruc.iir.rainbow.eva.invoker.InvokerWorkloadVectorEvaluation;
import cn.edu.ruc.iir.rainbow.layout.cmd.CmdGetColumnSize;
import cn.edu.ruc.iir.rainbow.layout.cmd.CmdOrdering;
import cn.edu.ruc.iir.rainbow.web.hdfs.common.SysConfig;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.Estimate;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.OrderedLayout;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.Pipeline;
import cn.edu.ruc.iir.rainbow.web.util.FileUtil;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.web.cmd
 * @ClassName: CmdReceiver
 * @Description: command received to run
 * @author: Tao
 * @date: Create in 2017-09-15 18:45
 **/
public class CmdReceiver {

    private static CmdReceiver instance = new CmdReceiver();
    private Pipeline pipeline;
    private OrderedLayout curLayout;
    private String targetPath;
    private String id;
    private String numRowGroup;
    private String orderedNumRowGroup;

    public OrderedLayout getCurLayout() {
        return curLayout;
    }

    public String getId() {
        return id;
    }

    public static CmdReceiver getInstance(Pipeline pipeline) {
        instance.pipeline = pipeline;
        instance.curLayout = instance.SearchLayoutByPno(pipeline.getNo());
        instance.targetPath = SysConfig.Catalog_Project + "pipeline/" + pipeline.getNo();
        instance.id = instance.searchEstimateByPno(pipeline.getNo());
        APCFactory apcFactory = APCFactory.Instance();
        AccessPatternCache APC = apcFactory.get(pipeline.getNo());
        if (APC == null) {
            APC = new AccessPatternCache(pipeline.getLifeTime(), pipeline.getThreshold()); // 100000, 0.1
            apcFactory.put(pipeline.getNo(), APC);
        }
        return instance;
    }

    private String searchEstimateByPno(String arg) {
        String id = "0";
        for (Estimate e : SysConfig.CurEstimate) {
            if (e.getNo().equals(arg)) {
                id = e.getId();
            }
        }
        return id;
    }

    private OrderedLayout SearchLayoutByPno(String no) {
        OrderedLayout layout = null;
        for (OrderedLayout o : SysConfig.CurOrderedLayout) {
            if (o.getNo().equals(no)) {
                layout = o;
                break;
            }
        }
        if (layout == null)
            layout = new OrderedLayout();
        return layout;
    }

    public void generateDDL(boolean ordered) {
        int count = curLayout.getCount();
        Invoker invoker = InvokerFactory.Instance().getInvoker(INVOKER.GENERATE_DDL);
        Properties params = new Properties();
        params.setProperty("file.format", pipeline.getFormat().toUpperCase());
        if (!ordered) {
            params.setProperty("table.name", pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_" + instance.id);
            params.setProperty("schema.file", targetPath + "/" + instance.id + "_schema.txt");
            params.setProperty("ddl.file", targetPath + "/" + pipeline.getFormat().toLowerCase() + "_" + instance.id + "_ddl.sql");
        } else {
            params.setProperty("table.name", pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_" + count);
            params.setProperty("schema.file", targetPath + "/" + count + "_schema.txt");
            params.setProperty("ddl.file", targetPath + "/" + pipeline.getFormat().toLowerCase() + "_" + count + "_ddl.sql");
        }
        try {
            invoker.executeCommands(params);
            if (!ordered) {
                params.setProperty("file.format", "TEXT");
                params.setProperty("table.name", pipeline.getNo());
                params.setProperty("ddl.file", targetPath + "/" + "text_ddl.sql");
                invoker.executeCommands(params);
            }
        } catch (InvokerException e) {
            e.printStackTrace();
        }
    }

    public void generateLoad(boolean ordered) {
        Invoker invoker = InvokerFactory.Instance().getInvoker(INVOKER.GENERATE_LOAD);
        Properties params = new Properties();
        params.setProperty("overwrite", "true");
        int count = curLayout.getCount();
        if (!ordered) {
            params.setProperty("schema.file", targetPath + "/" + instance.id + "_schema.txt");
            params.setProperty("load.file", targetPath + "/" + pipeline.getFormat().toLowerCase() + "_" + instance.id + "_load.sql");
            params.setProperty("table.name", pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_" + instance.id);
        } else {
            params.setProperty("schema.file", targetPath + "/" + count + "_schema.txt");
            params.setProperty("load.file", targetPath + "/" + pipeline.getFormat().toLowerCase() + "_" + count + "_load.sql");
            params.setProperty("table.name", pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_" + count);
        }
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
    }

    public void getColumnSize() {
        Properties params = new Properties();
        params.setProperty("file.format", pipeline.getFormat().toUpperCase());
        params.setProperty("schema.file", targetPath + "/" + id + "_schema.txt");
        params.setProperty("hdfs.table.path", SysConfig.Catalog_Sampling + pipeline.getNo() + "/ordered_" + instance.id);

        Command command = new CmdGetColumnSize();
        command.setReceiver(new Receiver() {
            @Override
            public void progress(double percentage) {
                String msg = "getColumnSize : " + ((int) (percentage * 10000) / 100.0) + " %    ";
                System.out.println(msg);
            }

            @Override
            public void action(Properties results) {
                instance.numRowGroup = results.getProperty("num.row.group");
                instance.orderedNumRowGroup = instance.numRowGroup;
                System.out.println("Finish. \tnum.row.group: " + instance.numRowGroup);
            }
        });

        command.execute(params);
    }

    public void ordering() {
        int count = curLayout.getCount();
        Properties params = new Properties();
        params.setProperty("algorithm.name", "scoa.gs");
        params.setProperty("schema.file", targetPath + "/" + instance.id + "_schema.txt");
        params.setProperty("ordered.schema.file", targetPath + "/" + count + "_schema.txt");
        params.setProperty("workload.file", targetPath + "/workload.txt");
        params.setProperty("seek.cost.function", "power");
        params.setProperty("computation.budget", "200");
        params.setProperty("num.row.group", instance.numRowGroup);
        params.setProperty("row.group.size", String.valueOf(pipeline.getRowGroupSize() * 1024 * 1024));  // "134217728" -> 128

        String filePath = targetPath + "/" + count + "_ordered.txt";
        Command command = new CmdOrdering();
        command.setReceiver(new Receiver() {
            @Override
            public void progress(double percentage) {
                String msg = "Layout Calculation : " + ((int) (percentage * 10000) / 100.0) + " %    ";
                System.out.println(msg);
                try {
                    FileUtil.writeFile(msg, filePath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void action(Properties results) {
                instance.orderedNumRowGroup = results.getProperty("num.row.group");
                System.out.println("Finish. \tnum.row.group: " + results.getProperty("row.group.size"));
            }
        });

        command.execute(params);
    }

    public void generateEstimation(boolean ordered) {
        Invoker invoker = InvokerFactory.Instance().getInvoker(INVOKER.PERFESTIMATION);
        Properties params = new Properties();
        params.setProperty("workload.file", targetPath + "/workload.txt");
        if (!ordered) {
            instance.getColumnSize();
            params.setProperty("schema.file", targetPath + "/" + instance.id + "_schema.txt");
            params.setProperty("log.file", targetPath + "/" + instance.id + "_estimate_duration.csv");
        } else {
            int count = curLayout.getCount();
            params.setProperty("schema.file", targetPath + "/" + count + "_schema.txt");
            params.setProperty("log.file", targetPath + "/" + count + "_estimate_duration.csv");
        }
        params.setProperty("num.row.group", instance.orderedNumRowGroup);
        params.setProperty("seek.cost.function", "power");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
    }

    public void WorkloadVectorEvaluation() {
        int count = curLayout.getCount();
        Properties params = new Properties();
        String method = ConfigFactory.Instance().getProperty("evaluation.method");
        params.setProperty("method", method);
        params.setProperty("format", pipeline.getFormat().toUpperCase());
        if (Integer.valueOf(instance.id) < count) {
            params.setProperty("table.dirs", SysConfig.Catalog_Sampling + pipeline.getNo() + "/ordered_" + instance.id + "," + SysConfig.Catalog_Sampling + pipeline.getNo() + "/ordered_" + count);
            params.setProperty("table.names", pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_" + instance.id + "," + pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_" + count);
        } else {
            params.setProperty("table.dirs", SysConfig.Catalog_Sampling + pipeline.getNo() + "/ordered_" + instance.id);
            params.setProperty("table.names", pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_" + instance.id);
        }
        params.setProperty("workload.file", targetPath + "/workload.txt");
        params.setProperty("log.dir", targetPath);
        params.setProperty("drop.cache", "false");
        params.setProperty("drop.caches.sh", "H:\\SelfLearning\\SAI\\DBIIR\\rainbow\\rainbow-evaluate\\src\\test\\resources\\drop_caches.sh");
        Invoker invoker = new InvokerWorkloadVectorEvaluation();
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
    }

    public boolean doAPC(String id, String arg, String weight) {
        boolean flag = false;
        APCFactory apcFactory = APCFactory.Instance();
        AccessPatternCache APC = apcFactory.get(pipeline.getNo());
        AccessPattern pattern = new AccessPattern(id, Double.valueOf(weight));
        for (String column : arg.split(",")) {
            pattern.addColumn(column);
        }
        if (APC.cache(pattern) && !SysConfig.APC_FLAG) {
            SysConfig.APC_FLAG = true;
            try {
                String time = DateUtil.formatTime(new Date());
                FileUtil.writeFile(time + "\t" + id + "\r\n", SysConfig.Catalog_Project + "APC.txt", true);
            } catch (IOException e) {
                e.printStackTrace();
            }
            APC.saveAsWorkloadFile(targetPath + "/workload.txt");
            flag = true;
        }
        return flag;
    }
}
