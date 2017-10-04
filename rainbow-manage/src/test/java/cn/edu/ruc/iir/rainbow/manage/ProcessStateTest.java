package cn.edu.ruc.iir.rainbow.manage;

import cn.edu.ruc.iir.rainbow.benchmark.util.DateUtil;
import cn.edu.ruc.iir.rainbow.manage.hdfs.common.SysConfig;
import cn.edu.ruc.iir.rainbow.manage.hdfs.model.Process;
import cn.edu.ruc.iir.rainbow.manage.hdfs.model.State;
import cn.edu.ruc.iir.rainbow.manage.util.FileUtil;
import com.alibaba.fastjson.JSONArray;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.manage
 * @ClassName: ProcessStateTest
 * @Description:
 * @author: Tao
 * @date: Create in 2017-09-17 10:52
 **/
public class ProcessStateTest {


    @Test
    public void ProcessTest() throws IOException {
        String time = DateUtil.formatTime(new Date());
        String state = SysConfig.PipelineState[0];

        State s = new State(time, state);
        List<State> pipelineState = new ArrayList<>();
        pipelineState.add(s);

        s = new State(DateUtil.formatTime(new Date()), SysConfig.PipelineState[1]);
        pipelineState.add(s);

        s = new State(DateUtil.formatTime(new Date()), SysConfig.PipelineState[2]);
        pipelineState.add(s);
        Process p = new Process("3fd97c0a9714cc7ea8d3277c535483cb", pipelineState);
        SysConfig.ProcessList.add(p);

        String aJson = JSONArray.toJSONString(SysConfig.ProcessList);
        FileUtil.writeFile(aJson, SysConfig.Catalog_Project + "cashe/process.txt");

    }
}
