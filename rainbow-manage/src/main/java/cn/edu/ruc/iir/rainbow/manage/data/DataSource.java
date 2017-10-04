package cn.edu.ruc.iir.rainbow.manage.data;

import cn.edu.ruc.iir.rainbow.manage.hdfs.model.Pipeline;
import cn.edu.ruc.iir.rainbow.manage.service.RwMain;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.manage.data
 * @ClassName: DataSource
 * @Description:
 * @author: Tao
 * @date: Create in 2017-09-19 16:35
 **/
public class DataSource {
    private RwMain rwMain;
    private String sourceUrl;
    private String storePath;

    public DataSource() {
    }


    public boolean getSampling(Pipeline pipeline) {
        return true;
    }

    public void loadData(Pipeline pipeline) {
    }

    public void loadDataToExamination(Pipeline pipeline, boolean ordered) {
    }
}
