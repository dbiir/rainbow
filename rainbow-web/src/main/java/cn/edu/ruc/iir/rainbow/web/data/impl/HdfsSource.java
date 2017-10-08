package cn.edu.ruc.iir.rainbow.web.data.impl;

import cn.edu.ruc.iir.rainbow.benchmark.util.SysSettings;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.web.data.DataSource;
import cn.edu.ruc.iir.rainbow.web.hdfs.common.SysConfig;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.Pipeline;
import cn.edu.ruc.iir.rainbow.web.hdfs.util.HdfsUtil;
import cn.edu.ruc.iir.rainbow.web.hive.util.HiveClient;
import cn.edu.ruc.iir.rainbow.web.service.RwMain;
import cn.edu.ruc.iir.rainbow.web.util.FileUtil;

import java.io.IOException;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.web.data.impl
 * @ClassName: HdfsSource
 * @Description:
 * @author: Tao
 * @date: Create in 2017-09-19 16:38
 **/
public class HdfsSource extends DataSource
{

    private RwMain rwMain;

    public HdfsSource()
    {
        rwMain = RwMain.Instance();
    }

    public boolean getSampling(Pipeline pipeline)
    {
        HdfsUtil hUtil = HdfsUtil.getHdfsUtil();
        int samplingSize = Integer.valueOf(ConfigFactory.Instance().getProperty("sampling.size"));
        List<String> listFile = null;
        try
        {
            listFile = hUtil.listAll(pipeline.getUrl());
            listFile = hUtil.listAll(listFile.get(listFile.size() - 1));
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        boolean flag = false;
        try
        {
            if (!hUtil.isTableExists(SysConfig.Catalog_Sampling + pipeline.getNo() + "/copy/sample"))
            {
//                flag = hUtil.copyContent(listFile.get(0), SysConfig.Catalog_Sampling + pipeline.getNo() + "/copy/sample", SysSettings.MB * samplingSize);
                hUtil.copyFile(listFile.get(0), SysConfig.Catalog_Sampling + pipeline.getNo() + "/copy/sample", false);
                flag = true;
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return flag;
    }

    public void loadData(Pipeline pipeline)
    {
        HiveClient client = HiveClient.Instance("jdbc:hive2://10.77.40.236:10000/default", "presto", "");
        HdfsUtil hUtil = HdfsUtil.getHdfsUtil();
        try
        {
            List<String> listFile = hUtil.listAll(pipeline.getUrl());
            String statement = FileUtil.readFile(SysConfig.Catalog_Project + "pipeline/" + pipeline.getNo() + "/text_ddl.sql");
            String statement1 = FileUtil.readFile(SysConfig.Catalog_Project + "pipeline/" + pipeline.getNo() + "/parquet_ddl.sql");
            String statement2 = FileUtil.readFile(SysConfig.Catalog_Project + "pipeline/" + pipeline.getNo() + "/parquet_load.sql");

            String sql = null;
            for (int i = listFile.size() - 1; i >= 0; i--)
            {
                client.drop("text");
                client.drop(pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_" + i);
                // check the state of the pipeline
                if (pipeline.getState() == 2)
                {
                    // stop
                    break;
                } else
                {
                    // accept, basic 0, 1, 3
                    sql = statement.replace("/rainbow/text", listFile.get(i));
                    client.execute(sql);
                    sql = statement1.replace("/rainbow/" + pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo(), pipeline.getStorePath() + i).replace(pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo(), pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_" + i) + getSqlParameter(pipeline);
                    client.execute(sql);
                    sql = statement2.replace(pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo(), pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_" + i);
                    client.execute(sql);
                }
                client.drop(pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_" + i);
                rwMain.getPipelineData();
                pipeline = rwMain.getPipelineByNo(pipeline.getNo(), 0);
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public void loadDataToExamination(Pipeline pipeline, boolean ordered)
    {
//        String method = ConfigFactory.Instance().getProperty("evaluation.method");
        HiveClient client = HiveClient.Instance("jdbc:hive2://10.77.40.236:10000/default", "presto", "");
        HdfsUtil hUtil = HdfsUtil.getHdfsUtil();
        String statement = FileUtil.readFile(SysConfig.Catalog_Project + "pipeline/" + pipeline.getNo() + "/text_ddl.sql");
        String statement1 = null, statement2 = null, sql = null;
        client.drop("text");
        sql = statement.replace("/rainbow/text", SysConfig.Catalog_Sampling + pipeline.getNo() + "/copy");
        client.execute(sql);
        String table = "";
        if (!ordered)
        {
            statement1 = FileUtil.readFile(SysConfig.Catalog_Project + "pipeline/" + pipeline.getNo() + "/" + pipeline.getFormat().toLowerCase() + "_ddl.sql");
            statement2 = FileUtil.readFile(SysConfig.Catalog_Project + "pipeline/" + pipeline.getNo() + "/" + pipeline.getFormat().toLowerCase() + "_load.sql");
            table = pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo();
            sql = statement1.replace("/rainbow/" + pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo(), SysConfig.Catalog_Sampling + pipeline.getNo() + "/origin") + getSqlParameter(pipeline);
        } else
        {
            statement1 = FileUtil.readFile(SysConfig.Catalog_Project + "pipeline/" + pipeline.getNo() + "/" + pipeline.getFormat().toLowerCase() + "_ordered_ddl.sql");
            statement2 = FileUtil.readFile(SysConfig.Catalog_Project + "pipeline/" + pipeline.getNo() + "/" + pipeline.getFormat().toLowerCase() + "_ordered_load.sql");
            table = pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_ordered";
            sql = statement1.replace("/rainbow/" + pipeline.getFormat().toLowerCase() + "_" + pipeline.getNo() + "_ordered", SysConfig.Catalog_Sampling + pipeline.getNo() + "/ordered") + getSqlParameter(pipeline);
        }
        client.execute(sql);
        client.execute(statement2);
//        if (!method.equals("presto"))
//            client.drop(table);
        client.drop("text");
    }

    private String getSqlParameter(Pipeline pipeline)
    {
        String sql = null;
        if (pipeline.getFormat().toLowerCase().equals("parquet"))
        {
            sql = "TBLPROPERTIES (\"parquet.block.size\"=\"" + pipeline.getRowGroupSize() * SysSettings.MB + "\", ";
            sql += "\"parquet.compression\"=\"" + pipeline.getCompression() + "\")";
        } else
        {
            sql = "TBLPROPERTIES (\"orc.stripe.size\"=\"" + pipeline.getRowGroupSize() * SysSettings.MB + "\", ";
            sql += "\"orc.compress\"=\"" + pipeline.getCompression() + "\")";
        }
        return sql;
    }


}
