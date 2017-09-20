package cn.edu.ruc.iir.rainbow.eva.metrics;

import cn.edu.ruc.iir.rainbow.common.exception.MetricsException;
import cn.edu.ruc.iir.rainbow.common.util.HttpFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.htmlparser.Parser;
import org.htmlparser.tags.TableColumn;
import org.htmlparser.tags.TableRow;
import org.htmlparser.tags.TableTag;
import org.htmlparser.util.ParserException;
import org.htmlparser.visitors.HtmlPage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hank on 1/28/2015.
 */
public class Crawler {
    private static Crawler instance = null;

    private Crawler () {
    }

    public static Crawler Instance ()
    {
        if (instance == null)
        {
            instance = new Crawler();
        }
        return  instance;
    }

    private Log log = LogFactory.getLog(this.getClass());

    public List<StageMetrics> getSparkV1StageMetricses (String ip, int port) throws MetricsException
    {
        String url = "http://" + ip + ":" + port + "/stages";
        String html = null;
        try {
            html = HttpFactory.getInstance().getPageHtml(url);
        } catch (IOException e) {
            log.error("Get html error:", e);
        }
        Parser parser = Parser.createParser(html, "utf-8");//spark web ui用的是utf-8编码
        HtmlPage page = new HtmlPage(parser);
        try {
            parser.visitAllNodesWith(page);
        } catch (ParserException e) {
            log.error("visit page error:", e);
        }

        TableTag[] tables = page.getTables();
        List<StageMetrics> stageMetricses = new ArrayList<StageMetrics>();
        TableTag table = null;
        if (tables.length == 3)
        {
            table = tables[1];//there are three table in the page if the job is finished.
        }
        else
        {
            throw new MetricsException("job not finished, stage metrics not found.");
        }

        for (TableRow row : table.getRows())
        {
            TableColumn[] columns = row.getColumns();
            StageMetrics metrics = new StageMetrics();
            metrics.setId(Integer.parseInt(columns[0].toPlainTextString().trim()));
            metrics.setDuration(Long.parseLong(columns[3].getAttribute("sorttable_customkey")));
            stageMetricses.add(metrics);
        }
        return stageMetricses;
    }

    public List<StageMetrics> getSparkV2StageMetricses (String ip, int port) throws MetricsException
    {
        String url = "http://" + ip + ":" + port + "/stages";
        String html = null;
        try {
            html = HttpFactory.getInstance().getPageHtml(url);
        } catch (IOException e) {
            log.error("Get html error:", e);
        }
        Parser parser = Parser.createParser(html, "utf-8");//spark web ui用的是utf-8编码
        HtmlPage page = new HtmlPage(parser);
        try {
            parser.visitAllNodesWith(page);
        } catch (ParserException e) {
            log.error("visit page error:", e);
        }

        TableTag[] tables = page.getTables();
        List<StageMetrics> stageMetricses = new ArrayList<StageMetrics>();
        TableTag table = null;
        if (tables.length == 1)
        {
            table = tables[0];//there are three table in the page if the job is finished.
        }
        else
        {
            throw new MetricsException("job not finished, stage metrics not found.");
        }

        for (TableRow row : table.getRows())
        {
            TableColumn[] columns = row.getColumns();
            StageMetrics metrics = new StageMetrics();
            metrics.setId(Integer.parseInt(columns[0].toPlainTextString().trim()));
            String unit = columns[3].toPlainTextString().trim().split(" ")[1].trim();
            long ms = 1000;
            if (unit.equalsIgnoreCase("s"))
            {
                ms = 1000;
            }
            else if (unit.equalsIgnoreCase("ms"))
            {
                ms = 1;
            }
            else if (unit.equalsIgnoreCase("min"))
            {
                ms = 60 * 1000;
            }
            else if (unit.equalsIgnoreCase("h"))
            {
                ms = 60 * 60 * 1000;
            }
            metrics.setDuration((long)(Double.parseDouble(columns[3].toPlainTextString().trim().split(" ")[0]) * ms));
            stageMetricses.add(metrics);
        }

        return stageMetricses;
    }

    /**
     * this function is not usable.
     * @param ip
     * @param port
     * @param stageId
     * @param attemptId
     * @param taskNum
     * @return
     * @throws MetricsException
     */
    public List<TaskMetrics> getAllTaskMetricses (String ip, int port, int stageId, int attemptId, int taskNum) throws MetricsException
    {
        String url = "http://" + ip + ":" + port + "/stages/stage/?id=" + stageId + "&attempt=" + attemptId;
        String html = null;
        try {
            html = HttpFactory.getInstance().getPageHtml(url);
        } catch (IOException e) {
            log.error("Get html error:", e);
        }
        Parser parser = Parser.createParser(html, "utf-8");//spark web ui用的是utf-8编码
        HtmlPage page = new HtmlPage(parser);
        try {
            parser.visitAllNodesWith(page);
        } catch (ParserException e) {
            log.error("visit page error:", e);
        }

        TableTag[] tables = page.getTables();

        List<TaskMetrics> taskMetricses = null;

        for (TableTag table : tables)
        {
            if (table.getRowCount() == taskNum)
            {
                taskMetricses = new ArrayList<TaskMetrics>();

                for (TableRow row : table.getRows())
                {
                    List<String> columns = new ArrayList<String>();
                    for (TableColumn column : row.getColumns())
                    {
                        //there is a problem, the plain text does not contain the value we need, we need the values in attributes
                        columns.add(column.toPlainTextString().trim());
                    }
                    TaskMetrics metrics = new TaskMetrics();
                    metrics.setId(Integer.parseInt(columns.get(TaskMetricsColumnId.getColumnId("id"))));
                    metrics.setDuration(Integer.parseInt(columns.get(TaskMetricsColumnId.getColumnId("duration"))));
                    //... add more task metrics to taskMetrics.

                    taskMetricses.add(metrics);
                }
                break;
            }
        }

        if (taskMetricses == null || taskMetricses.size() != taskNum)
        {
            throw new MetricsException("task metrics not found.");
        }
        return taskMetricses;
    }
}
