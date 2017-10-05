package cn.edu.ruc.iir.rainbow.web.controller;

import cn.edu.ruc.iir.rainbow.web.service.RwMain;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;

@CrossOrigin
@Controller
public class RwBasicAPI {


    @Autowired
    private RwMain rwMain;

    public RwBasicAPI() {

    }

    /**
     * Pipeline Create
     */
    @RequestMapping(value = "/schemaUpload", method = RequestMethod.POST)
    @ResponseBody
    public void schemaUpload(HttpServletRequest request, HttpServletResponse response) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException {
        rwMain.schemaUpload(request, response);
    }

    @RequestMapping(value = "/getDataUrl", method = RequestMethod.GET)
    @ResponseBody
    public String getDataUrl() {
        return rwMain.getDataUrl();
    }

    /**
     * Pipeline List
     */
    @RequestMapping(value = "/getPipelineData", method = RequestMethod.GET)
    @ResponseBody
    public String getPipelineData() throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException {
        return rwMain.getPipelineData();
    }

    @RequestMapping(value = "/delete", method = RequestMethod.POST)
    @ResponseBody
    public void delete(@RequestParam(value = "pno") String arg) {
        rwMain.delete(arg);
    }

    @RequestMapping(value = "/stop", method = RequestMethod.POST)
    @ResponseBody
    public void stop(@RequestParam(value = "pno") String arg) {
        rwMain.stop(arg);
    }

    /**
     * Sampling
     */
    @RequestMapping(value = "/startSampling", method = RequestMethod.POST)
    @ResponseBody
    public void startSampling(@RequestParam(value = "pno") String arg) {
        rwMain.startSampling(arg);
    }

    /**
     * Workload Upload
     */
    @RequestMapping(value = "/queryUpload", method = RequestMethod.POST)
    @ResponseBody
    public void queryUpload(@RequestParam(value = "query") String arg, @RequestParam(value = "pno") String pno) {
        rwMain.queryUpload(arg, pno);
    }

    @RequestMapping(value = "/clientUpload", method = RequestMethod.POST)
    @ResponseBody
    public String clientUpload(@RequestParam(value = "query") String arg, @RequestParam(value = "pno") String pno, @RequestParam(value = "id") String id, @RequestParam(value = "weight") String weight) {
        return rwMain.clientUpload(arg, pno, id, weight);
    }

    @RequestMapping(value = "/workloadUpload", method = RequestMethod.POST)
    @ResponseBody
    public void workloadUpload(HttpServletRequest request, HttpServletResponse response) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException {
        rwMain.workloadUpload(request, response);
    }

    /**
     * Layout Strategy
     */
    @RequestMapping(value = "/accept", method = RequestMethod.POST)
    @ResponseBody
    public void accept(@RequestParam(value = "pno") String arg) {
        rwMain.accept(arg);
    }

    @RequestMapping(value = "/optimization", method = RequestMethod.POST)
    @ResponseBody
    public void optimization(@RequestParam(value = "pno") String arg) {
        rwMain.optimization(arg);
    }

    @RequestMapping(value = "/getOrdered", method = RequestMethod.GET)
    @ResponseBody
    public String getOrdered(@RequestParam(value = "pno") String arg) {
        return rwMain.getOrdered(arg);
    }

    @RequestMapping(value = "/getEstimate_Sta", method = RequestMethod.GET)
    @ResponseBody
    public String getEstimate_Sta(@RequestParam(value = "pno") String arg) {
        return rwMain.getEstimate_Sta(arg);
    }

    @RequestMapping(value = "/getLayout", method = RequestMethod.GET)
    @ResponseBody
    public String getLayout(@RequestParam(value = "pno") String arg) {
        return rwMain.getLayout(arg);
    }

    /**
     * Evaluation
     */
    @RequestMapping(value = "/startEvaluation", method = RequestMethod.POST)
    @ResponseBody
    public void startEvaluation(@RequestParam(value = "pno") String arg) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException {
        rwMain.startEvaluation(arg);
    }

    @RequestMapping(value = "/getStatistic", method = RequestMethod.GET)
    @ResponseBody
    public String getStatistic(@RequestParam(value = "pno") String arg) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException {
        return rwMain.getStatistic(arg);
    }

    @RequestMapping(value = "/getQuery", method = RequestMethod.GET)
    @ResponseBody
    public String getQuery(@RequestParam(value = "rowid") String rowID, @RequestParam(value = "pno") String pno) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException {
        return rwMain.getQueryByRowID(Integer.valueOf(rowID), pno);
    }

    /**
     * Pipeline Process Timeline
     */
    @RequestMapping(value = "/getProcessState", method = RequestMethod.GET)
    @ResponseBody
    public String getProcessState(@RequestParam(value = "pno") String arg) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException {
        return rwMain.getProcessState(arg);
    }

    /**
     * Pipeline Process Detail
     */
    @RequestMapping(value = "/getPipelineDetail", method = RequestMethod.GET)
    @ResponseBody
    public String getPipelineDetail(@RequestParam(value = "pno") String pno, @RequestParam(value = "time") String time, @RequestParam(value = "desc") String desc) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException {
        return rwMain.getPipelineDetail(pno, time, desc);
    }

}
