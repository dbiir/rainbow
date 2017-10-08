package cn.edu.ruc.iir.rainbow.web.data;

import cn.edu.ruc.iir.rainbow.web.hdfs.model.Pipeline;
import cn.edu.ruc.iir.rainbow.web.service.RwMain;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.web.data
 * @ClassName: DataSource
 * @Description:
 * @author: Tao
 * @date: Create in 2017-09-19 16:35
 **/
public class DataSource
{
    private RwMain rwMain;
    private String sourceUrl;
    private String storePath;

    public DataSource()
    {
    }


    public boolean getSampling(Pipeline pipeline)
    {
        return true;
    }

    public void loadData(Pipeline pipeline)
    {
    }

    public void loadDataToExamination(Pipeline pipeline, boolean ordered)
    {
    }
}
