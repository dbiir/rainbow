package cn.edu.ruc.iir.rainbow.web;

import cn.edu.ruc.iir.rainbow.benchmark.util.DateUtil;
import cn.edu.ruc.iir.rainbow.benchmark.util.SysSettings;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.workload.AccessPattern;
import cn.edu.ruc.iir.rainbow.common.workload.AccessPatternCache;
import cn.edu.ruc.iir.rainbow.web.cmd.CmdReceiver;
import cn.edu.ruc.iir.rainbow.web.hdfs.common.SysConfig;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.Pipeline;
import cn.edu.ruc.iir.rainbow.web.hdfs.util.HdfsUtil;
import cn.edu.ruc.iir.rainbow.web.hive.util.HiveClient;
import cn.edu.ruc.iir.rainbow.web.service.RwMain;
import cn.edu.ruc.iir.rainbow.web.util.FileUtil;
import com.alibaba.fastjson.JSON;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.web
 * @ClassName: PipelineTest
 * @Description: test the whole schedule of one pipline
 * @author: Tao
 * @date: Create in 2017-09-27 14:40
 **/
public class PipelineTest {

    private RwMain rwMain = RwMain.Instance();

    private static Logger log = LoggerFactory.getLogger(PipelineTest.class);
    //    HdfsUtil hUtil = HdfsUtil.getHdfsUtil();
    String filePath = "/msra/text";
    String pno = "21dd00707735da8664e0ae471fab1e30";

    @Test
    public void CreateTest() {
        getDefaultInfo();
        Pipeline pipline = rwMain.getPipelineByNo(pno, 0);
        loadData(pipline);
    }

    public void loadData(Pipeline pipline) {
        HiveClient client = HiveClient.Instance("jdbc:hive2://10.77.40.236:10000/default", "presto", "");
        HdfsUtil hUtil = HdfsUtil.getHdfsUtil();
        try {
            List<String> listFile = hUtil.listAll(pipline.getUrl());
            String statement = FileUtil.readFile(SysConfig.Catalog_Project + "pipline/" + pipline.getNo() + "/text_ddl.sql");
            String statement1 = FileUtil.readFile(SysConfig.Catalog_Project + "pipline/" + pipline.getNo() + "/parquet_ddl.sql");
            String statement2 = FileUtil.readFile(SysConfig.Catalog_Project + "pipline/" + pipline.getNo() + "/parquet_load.sql");

            String sql = null;
            for (int i = listFile.size() - 1; i >= 0; i--) {
                client.drop("text");
                client.drop(pipline.getFormat().toLowerCase() + "_" + pipline.getNo() + "_" + i);
                // check the state of the pipline
                if (pipline.getState() == 2) {
                    // stop
                    break;
                } else {
                    // accept, basic 0, 1, 3
                    sql = statement.replace("/rainbow/text", listFile.get(i));
                    client.execute(sql);
                    sql = statement1.replace("/rainbow/" + pipline.getFormat().toLowerCase() + "_" + pipline.getNo(), pipline.getStorePath() + i).replace(pipline.getFormat().toLowerCase() + "_" + pipline.getNo(), pipline.getFormat().toLowerCase() + "_" + pipline.getNo() + "_" + i) + getSqlParameter(pipline);
                    client.execute(sql);
                    sql = statement2.replace(pipline.getFormat().toLowerCase() + "_" + pipline.getNo(), pipline.getFormat().toLowerCase() + "_" + pipline.getNo() + "_" + i);
                    client.execute(sql);
                }
                client.drop(pipline.getFormat().toLowerCase() + "_" + pipline.getNo() + "_" + i);
                getDefaultInfo();
                pipline = rwMain.getPipelineByNo(pipline.getNo(), 0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getDefaultInfo() {
        String aJson = FileUtil.readFile(SysConfig.Catalog_Project + "cashe/cashe.txt");
        SysConfig.PipelineList = JSON.parseArray(aJson,
                Pipeline.class);
    }

    private String getSqlParameter(Pipeline pipline) {
        String sql = null;
        if (pipline.getFormat().toLowerCase().equals("parquet")) {
            sql = "TBLPROPERTIES (\"parquet.block.size\"=\"" + pipline.getRowGroupSize() * SysSettings.MB + "\", ";
            sql += "\"parquet.compression\"=\"" + pipline.getCompression() + "\")";
        } else {
            sql = "TBLPROPERTIES (\"orc.stripe.size\"=\"" + pipline.getRowGroupSize() * SysSettings.MB + "\", ";
            sql += "\"orc.compress\"=\"" + pipline.getCompression() + "\")";
        }
        return sql;
    }

    @Test
    public void CreateLayoutTest() {
        getDefaultInfo();
        Pipeline pipline = rwMain.getPipelineByNo(pno, 0);
        rwMain.processLayout(pipline, SysConfig.PipelineState[0], false, false);
        rwMain.processLayout(pipline, SysConfig.PipelineState[6], false, false);
    }

    @Test
    public void GetLayoutTest() {
        String aJson = FileUtil.readFile(SysConfig.Catalog_Project + "pipline/" + pno + "/layout.txt");
        SysConfig.PipelineList = JSON.parseArray(aJson,
                Pipeline.class);
    }

    @Test
    public void SamplingTest() {
        getDefaultInfo();
        Pipeline p = rwMain.getPipelineByNo("96aa180e94dec692c0e49ca3bed86c38", 0);
        rwMain.getSampling(p, true);
    }

    @Test
    public void loadDataTest() {
        getDefaultInfo();
        Pipeline pipline = rwMain.getPipelineByNo("21dd00707735da8664e0ae471fab1e30", 0);
        rwMain.loadDataByPipeline(pipline);
    }

    @Test
    public void getLayoutTest() {
        getDefaultInfo();
        Pipeline p = rwMain.getPipelineByNo("dbced032765f68732a5caa949fb4a1df", 0);
        rwMain.getEstimation(p, false);
    }

    @Test
    public void getOptimizationTest() {
        getDefaultInfo();
        Pipeline p = rwMain.getPipelineByNo("dbced032765f68732a5caa949fb4a1df", 0);
        rwMain.getOptimization(p);
    }

    @Test
    public void getEstimationTest() {
        getDefaultInfo();
        Pipeline p = rwMain.getPipelineByNo("dbced032765f68732a5caa949fb4a1df", 0);
        rwMain.getEstimation(p, true);
    }

    @Test
    public void getSamplingTest() {
        getDefaultInfo();
        Pipeline p = rwMain.getPipelineByNo("dbced032765f68732a5caa949fb4a1df", 0);
        rwMain.getSampling(p, false);
    }

    @Test
    public void getEvaluationTest() {
        getDefaultInfo();
        Pipeline p = rwMain.getPipelineByNo("dbced032765f68732a5caa949fb4a1df", 0);
        rwMain.getEvaluation(p);
    }

    @Test
    public void getEstimationsTest() {
        String path = ConfigFactory.Instance().getProperty("pipline.path");
        SysConfig.Catalog_Project = path;
        getDefaultInfo();
        Pipeline p = rwMain.getPipelineByNo("4550483658f7985044256e09281f94c0", 0);
        CmdReceiver instance = CmdReceiver.getInstance(p);
        instance.generateEstimation(false);
        instance.generateEstimation(true);
    }

    @Test
    public void getEvaluationsTest() {
        String path = ConfigFactory.Instance().getProperty("pipline.path");
        SysConfig.Catalog_Project = path;
        getDefaultInfo();
        Pipeline p = rwMain.getPipelineByNo("4550483658f7985044256e09281f94c0", 0);
        CmdReceiver instance = CmdReceiver.getInstance(p);
        instance.WorkloadVectorEvaluation();
    }

    @Test
    public void test() throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader(
                "H:\\SelfLearning\\SAI\\DBIIR\\rainbows\\workload.txt"));
        String line = null;
        int i = 0, j = 0;
        Random random = new Random(System.currentTimeMillis());
        AccessPatternCache APC = new AccessPatternCache(4000, 0.1);
        while ((line = reader.readLine()) != null) {
            i++;
            String[] tokens = line.split("\t");
            double weight = Double.parseDouble(tokens[1]);
            AccessPattern pattern = new AccessPattern(tokens[0], weight);
            for (String column : tokens[2].split(",")) {
                pattern.addColumn(column);
            }

            if (APC.cache(pattern)) {
                System.out.println(DateUtil.formatTime(new Date()));
                System.out.println(i + ", trigger layout optimization.");
                j++;
                APC.saveAsWorkloadFile("H:\\SelfLearning\\SAI\\DBIIR\\rainbows\\workload_" + j + ".txt");
                System.out.println(DateUtil.formatTime(new Date()));
            }
            Thread.sleep(random.nextInt(20));
        }
    }

}
