package cn.edu.ruc.iir.rainbow.manage;

import cn.edu.ruc.iir.rainbow.benchmark.util.DateUtil;
import cn.edu.ruc.iir.rainbow.benchmark.util.SysSettings;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.manage.hdfs.common.SysConfig;
import cn.edu.ruc.iir.rainbow.manage.hdfs.util.HdfsUtil;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.manage
 * @ClassName: HdfsTest
 * @Description: Test HDFS util
 * @author: Tao
 * @date: Create in 2017-09-13 9:14
 **/
public class HdfsTest {

    private static Logger log = LoggerFactory.getLogger(HdfsTest.class);
    HdfsUtil hUtil = HdfsUtil.getHdfsUtil();

    String filePath = "/msra/text";

    @Test
    public void isTableExistsTest() {
        String filePath = SysConfig.Catalog_Cashe;
        try {
            Assert.assertEquals(false, hUtil.isTableExists(filePath));
        } catch (IOException e) {
            log.debug("Hdfs error info: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void listAllTest() {
        try {
            List<String> listFile = hUtil.listAll(filePath);
            Assert.assertEquals(true, listFile.size() > 0);
        } catch (IOException e) {
            log.debug("Hdfs error info: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    // append problem
    @Test
    public void appendContentTest() {
        try {
            List<String> listFile = hUtil.listAll(filePath);
            boolean flag = hUtil.copyContent(listFile.get(0), SysConfig.Catalog_Copy, SysSettings.MB * 10);
        } catch (IOException e) {
            log.debug("Hdfs error info: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void createFileTest() {
        try {
            List<String> listFile = hUtil.listAll(filePath);
//            boolean flag = hUtil.copyContent(listFile.get(0), "/msra/text/201709261830040.csv", SysSettings.MB * 100);
            boolean flag = hUtil.copyContent(listFile.get(0), "/msra/text/" + DateUtil.getCurTime() + ".csv", SysSettings.MB * 100);
            log.debug("Hdfs error info: {}", listFile.size());
        } catch (IOException e) {
            log.debug("Hdfs error info: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void copyContentTest() {
        try {
            int minBatch = Integer.valueOf(ConfigFactory.Instance().getProperty("sampling.size"));
            List<String> listFile = hUtil.listAll(filePath);
            boolean flag = hUtil.copyContent(listFile.get(0), SysConfig.Catalog_Copy, SysSettings.MB * minBatch);
        } catch (IOException e) {
            log.debug("Hdfs error info: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void copyFileTest() {
        try {
            List<String> listFile = hUtil.listAll(filePath);
            hUtil.copyFile(listFile.get(listFile.size() - 1), SysConfig.Catalog_Minibatch + "dbced032765f68732a5caa949fb4a1df", false);
        } catch (IOException e) {
            log.debug("Hdfs error info: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void copyMinibatchFileTest() {
        hUtil.copyFile("/rainbow-manage/evaluate/sampling/dbced032765f68732a5caa949fb4a1df/copy", "/rainbow-manage/evaluate/sampling/dbced032765f68732a5caa949fb4a1df/copy/sample", true);
    }
}
