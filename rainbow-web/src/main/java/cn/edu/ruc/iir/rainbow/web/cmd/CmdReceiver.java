package cn.edu.ruc.iir.rainbow.web.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import cn.edu.ruc.iir.rainbow.common.util.InputFactory;
import cn.edu.ruc.iir.rainbow.cli.INVOKER;
import cn.edu.ruc.iir.rainbow.cli.InvokerFactory;
import cn.edu.ruc.iir.rainbow.layout.cmd.CmdOrdering;
import cn.edu.ruc.iir.rainbow.web.hdfs.common.SysConfig;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.Pipeline;
import cn.edu.ruc.iir.rainbow.web.util.FileUtil;

import java.io.BufferedReader;
import java.io.IOException;
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
    private String targetPath;

    public static CmdReceiver getInstance(Pipeline pipeline) {
        instance.pipeline = pipeline;
        instance.targetPath = SysConfig.Catalog_Project + "pipeline/" + pipeline.getNo();
        return instance;
    }

    public void generateDDL(boolean ordered) {
        Invoker invoker = InvokerFactory.Instance().getInvoker(INVOKER.GENERATE_DDL);
        Properties params = new Properties();
        params.setProperty("file.format", pipeline.getFormat().toUpperCase());
        if (!ordered) {
            params.setProperty("table.name", pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo());
            params.setProperty("schema.file", targetPath + "/schema.txt");
            params.setProperty("ddl.file", targetPath + "/" + pipeline.getFormat().toLowerCase() + "_ddl.sql");
        } else {
            params.setProperty("table.name", pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_ordered");
            params.setProperty("schema.file", targetPath + "/schema_ordered.txt");
            params.setProperty("ddl.file", targetPath + "/" + pipeline.getFormat().toLowerCase() + "_ordered_ddl.sql");
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
        if (!ordered) {
            params.setProperty("schema.file", targetPath + "/schema.txt");
            params.setProperty("load.file", targetPath + "/" + pipeline.getFormat().toLowerCase() + "_load.sql");
            params.setProperty("table.name", pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo());
        } else {
            params.setProperty("schema.file", targetPath + "/schema_ordered.txt");
            params.setProperty("load.file", targetPath + "/" + pipeline.getFormat().toLowerCase() + "_ordered_load.sql");
            params.setProperty("table.name", pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_ordered");
        }
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
    }

    public void getColumnSize() {
        Invoker invoker = InvokerFactory.Instance().getInvoker(INVOKER.GET_COLUMN_SIZE);
        Properties params = new Properties();
        params.setProperty("file.format", pipeline.getFormat().toUpperCase());
        params.setProperty("schema.file", targetPath + "/schema.txt");
        params.setProperty("hdfs.table.path", SysConfig.Catalog_Sampling + pipeline.getNo() + "/origin");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
    }

    //    public void ordering() {
//        Invoker invoker = InvokerFactory.Instance().getInvoker(INVOKER.ORDERING);
//        Properties params = new Properties();
//        params.setProperty("algorithm.name", "scoa.gs");
//        params.setProperty("schema.file", targetPath + "/schema.txt");
//        params.setProperty("ordered.schema.file", targetPath + "/schema_ordered.txt");
//        params.setProperty("workload.file", targetPath + "/workload.txt");
//        params.setProperty("seek.cost.function", "power");
//        params.setProperty("computation.budget", "1000");
//        try {
//            invoker.executeCommands(params);
//        } catch (InvokerException e) {
//            e.printStackTrace();
//        }
//    }
    public void ordering() {
        Properties params = new Properties();
        params.setProperty("algorithm.name", "scoa.gs");
        params.setProperty("schema.file", targetPath + "/schema.txt");
        params.setProperty("ordered.schema.file", targetPath + "/schema_ordered.txt");
        params.setProperty("workload.file", targetPath + "/workload.txt");
        params.setProperty("seek.cost.function", "power");
        params.setProperty("computation.budget", "1000");

        String filePath = targetPath + "/ordered.txt";
        Command command = new CmdOrdering();
        command.setReceiver(new Receiver() {
            @Override
            public void progress(double percentage) {
                String msg = "Layout Calculation : " + ((int) (percentage * 10000) / 100.0) + " %    ";
                try {
                    FileUtil.writeFile(msg, filePath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void action(Properties results) {
                System.out.println("Finish.");
            }
        });

        command.execute(params);
    }

    public void generateEstimation(boolean ordered) {
//        Invoker invoker = InvokerFactory.Instance().getInvoker(INVOKER.PERFESTIMATION);
//        Properties params = new Properties();
//        params.setProperty("workload.file", targetPath + "/workload.txt");
//        if (!ordered) {
//            params.setProperty("schema.file", targetPath + "/schema.txt");
//            params.setProperty("log.file", targetPath + "/estimate_duration.csv");
//        } else {
//            params.setProperty("schema.file", targetPath + "/schema_ordered.txt");
//            params.setProperty("log.file", targetPath + "/estimate_duration_ordered.csv");
//        }
//        params.setProperty("seek.cost.function", "power");
//        try {
//            invoker.executeCommands(params);
//        } catch (InvokerException e) {
//            e.printStackTrace();
//        }
        String estimate;
        if (!ordered) {
            estimate = FileUtil.readFile(SysConfig.Catalog_Project + "estimate_duration.csv");
            try {
                FileUtil.writeFile(estimate, targetPath + "/estimate_duration.csv");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            estimate = FileUtil.readFile(SysConfig.Catalog_Project + "estimate_duration_ordered.csv");
            try {
                FileUtil.writeFile(estimate, targetPath + "/estimate_duration_ordered.csv");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void WorkloadVectorEvaluation() {
//        Properties params = new Properties();
//        String method = ConfigFactory.Instance().getProperty("evaluation.method");
//        params.setProperty("method", method);
//        params.setProperty("format", pipeline.getFormat().toUpperCase());
//        params.setProperty("table.dirs", SysConfig.Catalog_Sampling + pipeline.getNo() + "/origin" + "," + SysConfig.Catalog_Sampling + pipeline.getNo() + "/ordered");
//        params.setProperty("table.names", pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "," + pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_ordered");
//        params.setProperty("workload.file", targetPath + "/workload.txt");
//        params.setProperty("log.dir", targetPath);
//        params.setProperty("drop.cache", "false");
//        Invoker invoker = new InvokerWorkloadVectorEvaluation();
//        try {
//            invoker.executeCommands(params);
//        } catch (InvokerException e) {
//            e.printStackTrace();
//        }
        int num = 0;
        String filePath = SysConfig.Catalog_Project + "presto_duration.csv";
        try (BufferedReader reader = InputFactory.Instance().getReader(filePath)) {
            String line = reader.readLine();
            StringBuffer sb = new StringBuffer(line + "\n");
            FileUtil.writeFile(line, targetPath + "/presto_duration.csv");
            String s[];
            while ((line = reader.readLine()) != null) {
                s = line.split(",");
                sb.append(line + "\n");
                FileUtil.writeFile(sb.toString(), targetPath + "/presto_duration.csv");
                try {
                    Thread.sleep((Long.parseLong(s[1]) + Long.parseLong(s[2])) / 20);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
