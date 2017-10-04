package cn.edu.ruc.iir.rainbow.manage.service.impl;

import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.manage.hdfs.common.SysConfig;
import cn.edu.ruc.iir.rainbow.manage.hdfs.model.Pipeline;
import cn.edu.ruc.iir.rainbow.manage.hdfs.model.Process;
import cn.edu.ruc.iir.rainbow.manage.service.InitServiceI;
import cn.edu.ruc.iir.rainbow.manage.hdfs.util.HdfsUtil;
import cn.edu.ruc.iir.rainbow.manage.util.FileUtil;
import com.alibaba.fastjson.JSON;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;

@Service("demoInitService")
public class InitServiceImpl implements InitServiceI {

    @SuppressWarnings("unchecked")
    synchronized public void init() throws IOException {
        String path = ConfigFactory.Instance().getProperty("pipline.path");
        SysConfig.Catalog_Project = path;
        String filePath = SysConfig.Catalog_Project + "cashe";
        File file = new File(filePath);
        if (!file.exists()) {
            file.mkdirs();
        }
        HdfsUtil hUtil = HdfsUtil.getHdfsUtil();
        String aJson = FileUtil.readFile(SysConfig.Catalog_Project + "cashe/cashe.txt");
        if (aJson == "" || aJson == null) {
//            if (hUtil.isTableExists(SysConfig.Catalog_Cashe)) {
//                aJson = hUtil.readContent(SysConfig.Catalog_Cashe);
//                SysConfig.PipelineList = JSON.parseArray(aJson,
//                        Pipeline.class);
//            }
        } else {
            SysConfig.PipelineList = JSON.parseArray(aJson,
                    Pipeline.class);
        }


        aJson = FileUtil.readFile(SysConfig.Catalog_Project + "cashe/process.txt");
        if (aJson == "" || aJson == null) {
//            if (hUtil.isTableExists(SysConfig.Catalog_Cashe)) {
//                aJson = hUtil.readContent(SysConfig.Catalog_Cashe);
//                SysConfig.PipelineList = JSON.parseArray(aJson,
//                        Pipeline.class);
//            }
        } else {
            SysConfig.ProcessList = JSON.parseArray(aJson,
                    Process.class);
        }
    }

    /**
     * exec after web stopped
     */
    @PreDestroy
    public void applicationEnd() {
    }
}
