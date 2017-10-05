package cn.edu.ruc.iir.rainbow.redirect.receiver;

import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;

import java.util.Properties;

public class ReceiverRedirect implements Receiver
{
    /**
     * percentage is in range of (0, 1).
     * e.g. percentage=0.123 means 12.3%.
     *
     * @param percentage
     */
    @Override
    public void progress(double percentage)
    {
        System.out.print("\rREDIRECT: " + ((int)(percentage * 10000) / 100.0) + "%    ");
    }

    @Override
    public void action(Properties results)
    {
        System.out.println("\nFinish.\n");
        if (results.getProperty("query.id") != null)
        {
            System.out.println("Query ID: " + results.getProperty("query.id"));
        }
        System.out.println("Redirected access pattern: " + results.getProperty("access.pattern"));

    }
}
