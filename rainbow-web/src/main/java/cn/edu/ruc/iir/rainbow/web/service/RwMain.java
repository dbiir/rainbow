package cn.edu.ruc.iir.rainbow.web.service;

import cn.edu.ruc.iir.rainbow.benchmark.util.DateUtil;
import cn.edu.ruc.iir.rainbow.benchmark.util.SysSettings;
import cn.edu.ruc.iir.rainbow.common.exception.DataSourceException;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.InputFactory;
import cn.edu.ruc.iir.rainbow.web.cmd.CmdReceiver;
import cn.edu.ruc.iir.rainbow.web.data.DataSource;
import cn.edu.ruc.iir.rainbow.web.data.DataSourceFactory;
import cn.edu.ruc.iir.rainbow.web.hdfs.common.SysConfig;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.*;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.Process;
import cn.edu.ruc.iir.rainbow.web.server.UploadHandleServlet;
import cn.edu.ruc.iir.rainbow.web.util.FileUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.web
 * @ClassName: RwMain
 * @Description: Main service to support the front request
 * @author: Tao
 * @date: Create in 2017-09-12 23:17
 **/
public class RwMain
{

    private static RwMain instance = null;

    private RwMain()
    {

    }

    public static RwMain Instance()
    {
        if (instance == null)
        {
            instance = new RwMain();
        }
        return instance;
    }

    /**
     * Pipeline Create
     */
    public void schemaUpload(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        UploadHandleServlet uploadHandler = new UploadHandleServlet();
        List<String> names = uploadHandler.upload(request, response);
        Pipeline pipeline = new Pipeline(names);
        processLayout(pipeline, SysConfig.PipelineState[0], true);
        Thread t = new Thread(() -> processPipeline(pipeline));
        t.start();
        Thread t2 = new Thread(() -> beginSampling(pipeline));
        t2.start();
    }

    public void processLayout(Pipeline pipeline, String state, boolean flag)
    {
        // add state
        String time = DateUtil.formatTime(new Date());
        savePipelineState(pipeline, state, time);
        List<Layout> layouts = getLayoutInfo(pipeline);
        if(flag){
            Layout l = new Layout(pipeline, state, time);
            SysConfig.CurLayout.add(l);
//            layouts.add(l);   // flag is true -> curLayout.txt; false -> layout.txt
            String curLayoutList = JSONArray.toJSONString(SysConfig.CurLayout);
            try
            {
                FileUtil.writeFile(curLayoutList, SysConfig.Catalog_Project + "cashe/curLayout.txt");
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }else{
            Layout l = new Layout(pipeline, state, time);
            layouts.add(l);
        }
        String aJson = JSONArray.toJSONString(layouts);
        try
        {
            FileUtil.writeFile(aJson, SysConfig.Catalog_Project + "pipeline/" + pipeline.getNo() + "/layout.txt");
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private List<Layout> getLayoutInfo(Pipeline pipeline)
    {
        List<Layout> l = new ArrayList<Layout>(); // Layout lists
        String aJson = FileUtil.readFile(SysConfig.Catalog_Project + "pipeline/" + pipeline.getNo() + "/layout.txt");
        if (!aJson.equals(""))
        {
            l = JSON.parseArray(aJson,
                    Layout.class);
        }
        return l;
    }

    private void processPipeline(Pipeline pipeline)
    {
        SysConfig.PipelineList.add(pipeline);
        String aJson = JSONArray.toJSONString(SysConfig.PipelineList);
        // write to local
        try
        {
            FileUtil.writeFile(aJson, SysConfig.Catalog_Project + "cashe/cashe.txt");
//            HdfsUtil hdfsUtil = HdfsUtil.getHdfsUtil();
            // write to hdfs
//        hdfsUtil.copyFile(SysConfig.Catalog_Project + "cashe/cashe.txt", SysConfig.Catalog_Cashe);
//        hdfsUtil.upFile(names.get(2), SysConfig.Catalog_Pipeline +"/"+ names.get(6) + "/schema.txt");
            // GENERATE_DDL, GENERATE_LOAD
            CmdReceiver instance = CmdReceiver.getInstance(pipeline);
            instance.generateDDL(false);
            instance.generateLoad(false);
            // load data in producing clusters(mini batch)
            savePipelineState(pipeline, SysConfig.PipelineState[1]);
            changeState(pipeline.getNo(), 1);
//            loadDataByPipeline(pipeline);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public void loadDataByPipeline(Pipeline pipeline)
    {
        DataSourceFactory dsf = DataSourceFactory.Instance();
        try
        {
            DataSource ds = dsf.getDataSource(pipeline.getDataSource());
            ds.loadData(pipeline);
        } catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        } catch (DataSourceException e)
        {
            e.printStackTrace();
        }
    }

    public String getDataUrl()
    {
        return ConfigFactory.Instance().getProperty("datasource");
    }

    /**
     * Pipeline List
     */
    public String getPipelineData()
    {
        String aJson = FileUtil.readFile(SysConfig.Catalog_Project + "cashe/cashe.txt");
        if (aJson.length() > 0)
            SysConfig.PipelineList = JSON.parseArray(aJson,
                    Pipeline.class);
        return JSON.toJSONString(SysConfig.PipelineList);
    }

    public void savePipelineState(Pipeline pipeline, String state)
    {
        String time = DateUtil.formatTime(new Date());
        State s = new State(time, state);
        Process p = searchProcessByPno(pipeline.getNo());
        if (p != null)
        {
            p.getPipelineState().add(s);
        } else
        {
            List<State> PipelineState = new ArrayList<>();
            PipelineState.add(s);
            p = new Process(pipeline.getNo(), PipelineState);
            SysConfig.ProcessList.add(p);
        }
        String processListJson = JSONArray.toJSONString(SysConfig.ProcessList);
        try
        {
            FileUtil.writeFile(processListJson, SysConfig.Catalog_Project + "cashe/process.txt");
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private void savePipelineState(Pipeline pipeline, String state, String time)
    {
        State s = new State(time, state);
        Process p = searchProcessByPno(pipeline.getNo());
        if (p != null)
        {
            p.getPipelineState().add(s);
        } else
        {
            List<State> PipelineState = new ArrayList<>();
            PipelineState.add(s);
            p = new Process(pipeline.getNo(), PipelineState);
            SysConfig.ProcessList.add(p);
        }
        String processListJson = JSONArray.toJSONString(SysConfig.ProcessList);
        try
        {
            FileUtil.writeFile(processListJson, SysConfig.Catalog_Project + "cashe/process.txt");
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public void delete(String no)
    {
        int i = 0;
        for (Pipeline p : SysConfig.PipelineList)
        {
            if (p.getNo().equals(no))
            {
                SysConfig.PipelineList.remove(i);
                break;
            }
            i++;
        }
        i = 0;
        for (Process p : SysConfig.ProcessList)
        {
            if (p.getPipelineNo().equals(no))
            {
                SysConfig.ProcessList.remove(i);
                break;
            }
            i++;
        }
        String path = SysConfig.Catalog_Project + "pipeline/" + no;
        FileUtil.delDirectory(path);
        Thread t = new Thread(() -> updatePipelineList(true));
        t.start();
    }

    private void updatePipelineList(boolean flag)
    {
        String aJson = JSONArray.toJSONString(SysConfig.PipelineList);
        try
        {
            FileUtil.writeFile(aJson, SysConfig.Catalog_Project + "cashe/cashe.txt");
            // remove process
            if(flag){
                aJson = JSONArray.toJSONString(SysConfig.ProcessList);
                FileUtil.writeFile(aJson, SysConfig.Catalog_Project + "cashe/process.txt");
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public void stop(String no)
    {
        for (Pipeline p : SysConfig.PipelineList)
        {
            if (p.getNo().equals(no))
            {
                p.setState(2);
                break;
            }
        }
        updatePipelineList(false);
    }

    public Process searchProcessByPno(String pno)
    {
        Process p1 = null;
        for (Process p : SysConfig.ProcessList)
        {
            if (p.getPipelineNo().equals(pno))
            {
                p1 = p;
                break;
            }
        }
        return p1;
    }

    public Pipeline getPipelineByNo(String no, int state)
    {
        Pipeline pipeline = new Pipeline();
        for (Pipeline p : SysConfig.PipelineList)
        {
            if (p.getNo().equals(no))
            {
                pipeline = p;
                if (state != 0)
                {
                    p.setState(state);
                }
                break;
            }
        }
        return pipeline;
    }

    public void beginSampling(Pipeline pipeline)
    {
        savePipelineState(pipeline, SysConfig.PipelineState[2]);
        // copy mini batch (thread, sampling) in evaluating cluster
        getSampling(pipeline, true);
    }

    /**
     * Sampling
     */
    public void startSampling(String arg)
    {
        Pipeline pipeline = getPipelineByNo(arg, 1);
        savePipelineState(pipeline, SysConfig.PipelineState[2]);
        // copy mini batch (thread, sampling) in evaluating cluster
        Thread t = new Thread(() -> getSampling(pipeline, true));
        t.start();
    }

    // default: sampling size <= list[i] size
    public void getSampling(Pipeline pipeline, boolean sample)
    {
        DataSourceFactory dsf = DataSourceFactory.Instance();
        try
        {
            DataSource ds = dsf.getDataSource(pipeline.getDataSource());
            if (sample && ds.getSampling(pipeline))
            {
                ds.loadDataToExamination(pipeline, false);
                savePipelineState(pipeline, SysConfig.PipelineState[3]);
            }if (sample && !ds.getSampling(pipeline))
            {
//                ds.loadDataToExamination(pipeline, false);
                savePipelineState(pipeline, SysConfig.PipelineState[3]);
            } else
            {
                ds.loadDataToExamination(pipeline, true);
            }
        } catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        } catch (DataSourceException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Workload Upload
     */
    public void queryUpload(String arg, String pno)
    {
        String realSavePath = SysConfig.Catalog_Project + "\\pipeline\\" + pno + "/" + "\\workload.txt";
        String line = UUID.randomUUID() + "\t1\t" + arg + "\r\n";
        try
        {
            FileUtil.writeFile(line, realSavePath, true);
            Thread t = new Thread(() -> changeState(pno, 1));
            t.start();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public String clientUpload(String arg, String pno, String id, String weight)
    {
        String res = "";
        String realSavePath = SysConfig.Catalog_Project + "\\pipeline\\" + pno + "/" + "workload.txt";
        String line = id + "\t" + weight + "\t" + arg + "\r\n";
        try
        {
//            FileUtil.writeFile(line, realSavePath, true);
            res = "{\"Res\":\"OK\"}";
            Thread t = new Thread(() -> changeState(pno, 1));
            t.start();

            Thread t1 = new Thread(() -> getEstimation(arg, pno, id, weight, false));
            t1.start();
        } catch (Exception e)
        {
            res = "{\"Res\":\"Error\"}";
            e.printStackTrace();
        }
        return res;
    }

    private void changeState(String pno, int state)
    {
        for (Pipeline p : SysConfig.PipelineList)
        {
            if (p.getNo().equals(pno))
            {
                p.setState(state);
                break;
            }
        }
        String aJson = JSONArray.toJSONString(SysConfig.PipelineList);
        try
        {
            FileUtil.writeFile(aJson, SysConfig.Catalog_Project + "cashe/cashe.txt");
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public void workloadUpload(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        UploadHandleServlet uploadHandler = new UploadHandleServlet();
        List<String> names = uploadHandler.upload(request, response);
        Pipeline pipeline = getPipelineByNo(names.get(1), 0);
        savePipelineState(pipeline, SysConfig.PipelineState[4]);
        Thread t = new Thread(() -> getEstimation(pipeline, false));
        t.start();
    }

    public void getEstimation(Pipeline pipeline, boolean ordered)
    {
        // PerfEstimation
        CmdReceiver instance = CmdReceiver.getInstance(pipeline);
        instance.generateEstimation(ordered);
        if (ordered)
        {
            pipeline = updatePipelineByGs(pipeline);
            processLayout(pipeline, SysConfig.PipelineState[7], false);
        } else
        {
            savePipelineState(pipeline, SysConfig.PipelineState[5]);
            beginOptimizing(pipeline);
        }
    }

    public void getEstimation(String arg, String pno, String id, String weight, boolean ordered)
    {
        Pipeline pipeline = getPipelineByNo(pno, 0);
        CmdReceiver instance = CmdReceiver.getInstance(pipeline);
        if (ordered)
        {
        } else
        {
            boolean flag = instance.doAPC(id, arg, weight);
            if(flag){
                instance.generateEstimation(ordered);
                beginOptimizing(pipeline);
            }
        }
    }
    private Pipeline updatePipelineByGs(Pipeline p)
    {
        String filePath = SysConfig.Catalog_Project + "pipeline/" + p.getNo() + "/schema_ordered.txt.gs";
        p.setColumnOrder(1);
        File f = new File(filePath);
        if (!f.exists())
        {

        } else
        {
            try (BufferedReader reader = InputFactory.Instance().getReader(filePath))
            {
                String line = reader.readLine();
                String[] splits = line.split("=");  //  row.group.size=1073741824
                p.setRowGroupSize((int) (Double.valueOf(splits[1]) / SysSettings.MB));
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        return p;
    }

    /**
     * Layout Strategy
     */
    public void optimization(String arg)
    {
        Pipeline pipeline = getPipelineByNo(arg, 1);
        savePipelineState(pipeline, SysConfig.PipelineState[6]);
        Thread t = new Thread(() -> getOptimization(pipeline));
        t.start();
    }

    public void beginOptimizing(Pipeline pipeline)
    {
        savePipelineState(pipeline, SysConfig.PipelineState[6]);
        Thread t = new Thread(() -> getOptimization(pipeline));
        t.start();
    }

    public void accept(String arg)
    {
        Pipeline pipeline = getPipelineByNo(arg, 3);
        pipeline = updatePipelineByGs(pipeline);
        // Accept Optimization (change the state)
        Pipeline finalPipeline = pipeline;
        Thread t = new Thread(() -> processLayout(finalPipeline, SysConfig.PipelineState[10], false));
        t.start();
        Thread t1 = new Thread(() -> updateLayout(finalPipeline));
        t1.start();
    }

    private void updateLayout(Pipeline finalPipeline) {
        for(Layout layout: SysConfig.CurLayout){
            if (layout.getNo().equals(finalPipeline.getNo())){
                String time = DateUtil.formatTime(new Date());
                layout = new Layout(finalPipeline, time);
                break;
            }
        }
        saveCurLayout();
    }

    private void saveCurLayout() {
        String aJson = JSONArray.toJSONString(SysConfig.CurLayout);
        try
        {
            FileUtil.writeFile(aJson, SysConfig.Catalog_Project + "cashe/curLayout.txt");
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public void getOptimization(Pipeline pipeline)
    {
        // GET_COLUMN_SIZE, ORDERING
        CmdReceiver instance = CmdReceiver.getInstance(pipeline);
        instance.getColumnSize();
        instance.ordering();
        SysConfig.APC_FLAG = false;

        //  ordered
        instance.generateDDL(true);
        instance.generateLoad(true);
        Thread t = new Thread(() -> getSampling(pipeline, false));
        t.start();
        // PerfEstimation_Ordered
        Thread t1 = new Thread(() -> getEstimation(pipeline, true));
        t1.start();
    }

    public String getOrdered(String arg)
    {
        String filePath = SysConfig.Catalog_Project + "pipeline/" + arg + "/ordered.txt";
        File f = new File(filePath);
        if (!f.exists())
        {
            return "";
        }
        String msg = FileUtil.readFile(filePath);
        return msg;
    }

    public String getEstimate_Sta(String arg)
    {
        String filePath = SysConfig.Catalog_Project + "pipeline/" + arg + "/estimate_duration.csv";
        File f = new File(filePath);
        if (!f.exists())
        {
            return "[]";
        }
        List<Statistic> list = new ArrayList<Statistic>();
        Statistic s1 = getStatisticByFilePath(filePath, "Current");
        list.add(s1);
        filePath = SysConfig.Catalog_Project + "pipeline/" + arg + "/estimate_duration_ordered.csv";
        f = new File(filePath);
        if (f.exists())
        {
            Statistic s2 = getStatisticByFilePath(filePath, "Optimized");
            list.add(s2);
        }
        return JSON.toJSONString(list);
    }

    private Statistic getStatisticByFilePath(String filePath, String name)
    {
        DecimalFormat df = new DecimalFormat("######0.00");
        Statistic s1 = null;
        try (BufferedReader reader = InputFactory.Instance().getReader(filePath))
        {
            String line = reader.readLine();
            String[] splits;
            int i = 0;
            List<double[]> li1 = new ArrayList<double[]>();
            while ((line = reader.readLine()) != null)
            {
                splits = line.split(",");
                double[] s = {Double.valueOf(i), Double.valueOf(df.format(Double.valueOf(splits[1]) / 1000))};
                li1.add(s);
                i++;
            }
            s1 = new Statistic(name, li1);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return s1;
    }

    public String getLayout(String arg)
    {
        String filePath = SysConfig.Catalog_Project + "pipeline/" + arg + "/layout.txt";
        File f = new File(filePath);
        if (!f.exists())
        {
            return "[]";
        }
        String aJson = FileUtil.readFile(filePath);
        SysConfig.PipelineLayout = JSON.parseArray(aJson, Layout.class);
        return JSON.toJSONString(SysConfig.PipelineLayout);
    }

    public String getCurrentLayout(String arg)
    {
        Layout layout = null;
        boolean flag = false;
        for (Layout l : SysConfig.CurLayout){
            if(l.getNo().equals(arg)){
                layout = l;
                flag = true;
            }
        }
        if(!flag){
            layout = new Layout();
        }
        return JSON.toJSONString(layout);
    }
    /**
     * Evaluation
     */
    public void startEvaluation(String arg)
    {
        Pipeline pipeline = getPipelineByNo(arg, 1);
        savePipelineState(pipeline, SysConfig.PipelineState[8]);
        // Evaluate (rainbow-evaluate)
        Thread t = new Thread(() -> getEvaluation(pipeline));
        t.start();
    }

    public void getEvaluation(Pipeline pipeline)
    {
        CmdReceiver instance = CmdReceiver.getInstance(pipeline);
        // ordered
        instance.WorkloadVectorEvaluation();
        savePipelineState(pipeline, SysConfig.PipelineState[9]);
    }

    public String getStatistic(String arg)
    {
        DecimalFormat df = new DecimalFormat("######0.00");
        String method = ConfigFactory.Instance().getProperty("evaluation.method");
        String filePath = SysConfig.Catalog_Project + "pipeline/" + arg + "/";
        if (method.equals("SPARK2"))
            filePath += "spark_duration.csv";
        else
        {
            filePath += "presto_duration.csv";
        }
        File f = new File(filePath);
        if (!f.exists())
        {
            return "[]";
        }
        List<Statistic> list = new ArrayList<Statistic>();
        try (BufferedReader reader = InputFactory.Instance().getReader(filePath))
        {
            String line = reader.readLine();
            String[] splits;
            int i = 0;
            List<double[]> li1 = new ArrayList<double[]>();
            List<double[]> li2 = new ArrayList<double[]>();
            while ((line = reader.readLine()) != null)
            {
                splits = line.split(",");
                if (splits.length == 2)
                {
                    double[] s = {Double.valueOf(i), Double.valueOf(splits[1])};
                    li1.add(s);
                } else
                {
                    double[] s = {Double.valueOf(i), Double.valueOf(df.format(Double.valueOf(splits[1]) / 1000))};
                    double[] s1 = {Double.valueOf(i), Double.valueOf(df.format(Double.valueOf(splits[2]) / 1000))};
                    li1.add(s);
                    li2.add(s1);
                }
                i++;
            }
            Statistic s1, s2;
            s1 = new Statistic("Current", li1);
            list.add(s1);
            if (li2.size() > 0)
            {
                s2 = new Statistic("Optimized", li2);
                list.add(s2);
            } else
            {

            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return JSON.toJSONString(list);
    }

    public String getQueryByRowID(int rowID, String pno)
    {
        String filePath = SysConfig.Catalog_Project + "pipeline/" + pno + "/workload.txt";
        String query = null;
        try (BufferedReader reader = InputFactory.Instance().getReader(filePath))
        {
            String line = reader.readLine();
            String[] splits;
            int i = 0;
            while ((line = reader.readLine()) != null)
            {
                if (i == rowID)
                {
                    splits = line.split("\t");
                    query = splits[2];
                    break;
                }
                i++;
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return query;
    }

    /**
     * Pipeline Process Timeline
     */
    public String getProcessState(String arg)
    {
        String aJson = FileUtil.readFile(SysConfig.Catalog_Project + "cashe/process.txt");
        SysConfig.ProcessList = JSON.parseArray(aJson, Process.class);
        return JSON.toJSONString(SysConfig.ProcessList);
    }

    /**
     * Pipeline Process Detail
     */
    public String getPipelineDetail(String pno, String time, String desc)
    {
        Pipeline p = getPipelineByNo(pno, 0);
        String filePath = SysConfig.Catalog_Project + "pipeline/" + pno + "/layout.txt";
        String aJson = FileUtil.readFile(filePath);
        List<Layout> l = new ArrayList<Layout>(); // Layout lists
        if (aJson.length() > 0)
        {
            l = JSON.parseArray(aJson,
                    Layout.class);
        }
        return JSON.toJSONString(l);
    }
}