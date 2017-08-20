package cn.edu.ruc.iir.rainbow.eva;

import cn.edu.ruc.iir.rainbow.common.exception.MetricsException;
import cn.edu.ruc.iir.rainbow.eva.metrics.Crawler;
import cn.edu.ruc.iir.rainbow.eva.metrics.StageMetrics;
import org.junit.Test;

import java.util.List;

/**
 * Created by hank on 2015/2/5.
 */
public class TestCrawler
{
    @Test
    void testCrawling ()
    {
        try
        {
            List<StageMetrics> metricses =  Crawler.Instance().getAllStageMetricses("10.172.96.77", 4040);
            for (StageMetrics metrics : metricses)
            {
                System.out.println(metrics.getId() + ", " + metrics.getDuration());
            }
        } catch (MetricsException e)
        {
            e.printStackTrace();
        }
    }
}
