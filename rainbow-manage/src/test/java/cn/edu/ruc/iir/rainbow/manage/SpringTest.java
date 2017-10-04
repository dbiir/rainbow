package cn.edu.ruc.iir.rainbow.manage;

import cn.edu.ruc.iir.rainbow.manage.service.InitServiceI;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.manage
 * @ClassName: SpringTest
 * @Description: test the frame
 * @author: Tao
 * @date: Create in 2017-09-13 8:3
 * 0
 **/
public class SpringTest {

    private static Logger log = LoggerFactory.getLogger(SpringTest.class);
    private static ApplicationContext ctx = null;

    @Test
    public void ServiceTest() {
        ctx = new ClassPathXmlApplicationContext("applicationContext.xml");
        InitServiceI initService = (InitServiceI) ctx
                .getBean("demoInitService");
        try {
            initService.init();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void CatalogTest() {
        String relativelyPath = System.getProperty("user.dir");
        System.out.println(relativelyPath);
    }
}
