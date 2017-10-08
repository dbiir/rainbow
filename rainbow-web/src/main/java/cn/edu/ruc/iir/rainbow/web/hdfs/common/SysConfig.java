package cn.edu.ruc.iir.rainbow.web.hdfs.common;


import cn.edu.ruc.iir.rainbow.web.hdfs.model.*;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.Process;

import java.util.ArrayList;
import java.util.List;

public class SysConfig
{
    public static String Catalog_Project;

    public static final String Catalog_Cashe = "/rainbow-web/cashe/cashe.txt";
    public static final String Catalog_Copy = "/rainbow-web/evaluate/minibatch/copy";
    public static final String Catalog_Minibatch = "/rainbow-web/evaluate/minibatch/";
    public static final String Catalog_Sampling = "/rainbow-web/evaluate/sampling/";

    public static List<Pipeline> PipelineList = new ArrayList<Pipeline>(); // Pipeline lists
    public static List<Process> ProcessList = new ArrayList<Process>(); // Process lists
    public static List<Layout> PipelineLayout = new ArrayList<Layout>(); // Layout lists
    public static String[] PipelineState = {"Pipeline Created", "Data Loading Started", "Sampling Started", "Sampling Finished", "Workload Uploading Started", "Workload Uploading Finished", "Optimization Started", "Optimization Finished", "Evaluation Started", "Evaluation Finished", "Accepting Optimized Strategy", "Stopped", "Removed"};
}
