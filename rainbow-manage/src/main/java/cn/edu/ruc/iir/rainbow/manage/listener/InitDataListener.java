package cn.edu.ruc.iir.rainbow.manage.listener;

import cn.edu.ruc.iir.rainbow.manage.hdfs.util.HdfsUtil;
import cn.edu.ruc.iir.rainbow.manage.service.InitServiceI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;

public class InitDataListener implements ServletContextListener {

    private static Logger log = LoggerFactory.getLogger(InitServiceI.class);

    private static ApplicationContext ctx = null;

    public InitDataListener() {
    }

    @Override
    public void contextDestroyed(ServletContextEvent evt) {
        HdfsUtil hUtil = HdfsUtil.getHdfsUtil();
        try {
            hUtil.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.debug("Close the Rainbow-manage");
    }

    @Override
    public void contextInitialized(ServletContextEvent evt) {
        log.debug("Load data");
        ctx = new ClassPathXmlApplicationContext("applicationContext.xml");
        InitServiceI initService = (InitServiceI) ctx
                .getBean("demoInitService");
        try {
            initService.init();
            // File file = new File(this.getClass().getClassLoader()
            // .getResource(("applicationContext.xml")).getFile());
            // SysConfig.Catalog_Project = file.getAbsolutePath().replace(
            // "applicationContext.xml", "");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
