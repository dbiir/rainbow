package cn.edu.ruc.iir.rainbow.web;

import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.web.hdfs.common.SysConfig;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.Layout;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.Pipeline;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.Process;
import cn.edu.ruc.iir.rainbow.web.service.RwMain;
import cn.edu.ruc.iir.rainbow.web.util.FileUtil;
import com.alibaba.fastjson.JSON;
import org.junit.jupiter.api.Test;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.web
 * @ClassName: AdaptiveTest
 * @Description: To make a adaptive test
 * @author: taoyouxian
 * @date: Create in 2017-10-28 18:43
 **/
public class AdaptiveTest {

    private RwMain rwMain = RwMain.Instance();

    private String no = "54ba30d7abdbe13ab2c886f18c0f5555";
    String path = ConfigFactory.Instance().getProperty("pipline.path");

    @Test
    public void SamplingTest() {
        getDefaultInfo();
        Pipeline p = rwMain.getPipelineByNo(no, 0);
        rwMain.getSampling(p, true);
    }

    @Test
    public void deleteTest() {
        getDefaultInfo();
        rwMain.delete(no);
    }

    public void getDefaultInfo() {
        SysConfig.Catalog_Project = path;
        String aJson = FileUtil.readFile(path + "cashe/cashe.txt");
        SysConfig.PipelineList = JSON.parseArray(aJson,
                Pipeline.class);
        aJson = FileUtil.readFile(path + "cashe/process.txt");
        SysConfig.ProcessList = JSON.parseArray(aJson,
                Process.class);
        aJson = FileUtil.readFile(path + "cashe/curLayout.txt");
        SysConfig.CurLayout = JSON.parseArray(aJson,
                Layout.class);
    }

    @Test
    public void getEstimationTest() {
        getDefaultInfo();
        Pipeline p = rwMain.getPipelineByNo(no, 0);
        rwMain.getEstimation(p, false);
    }

    @Test
    public void getCurrentLayoutTest() {
        getDefaultInfo();
        String aJson = rwMain.getCurrentLayout("1");
        System.out.println(aJson);
    }

}
