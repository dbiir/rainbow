package cn.edu.ruc.iir.rainbow.manage.data;

import cn.edu.ruc.iir.rainbow.common.exception.DataSourceException;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.LogFactory;
import org.apache.commons.logging.Log;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.manage.data
 * @ClassName: DataSourceFactory
 * @Description:
 * @author: Tao
 * @date: Create in 2017-09-19 16:59
 **/
public class DataSourceFactory {


    private static DataSourceFactory instance = null;

    private DataSourceFactory() {

    }

    public static DataSourceFactory Instance() {
        if (instance == null) {
            instance = new DataSourceFactory();
        }
        return instance;
    }

    private Log log = LogFactory.Instance().getLog();

    public DataSource getDataSource(String dataName) throws ClassNotFoundException, DataSourceException {
        String className = ConfigFactory.Instance().getProperty(dataName);
        Class<?> dataClass = Class.forName(className);
        DataSource dataSource = null;
        try {
            dataSource = (DataSource) dataClass.newInstance();
        } catch (Exception e) {
            log.error("data construction error: ", e);
            throw new DataSourceException("data class does not have a non-param constructor.");
        }
        return dataSource;
    }

}
