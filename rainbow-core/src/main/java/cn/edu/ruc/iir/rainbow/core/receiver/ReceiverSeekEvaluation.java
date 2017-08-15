package cn.edu.ruc.iir.rainbow.core.receiver;

import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;

import java.util.Properties;

public class ReceiverSeekEvaluation implements Receiver
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
        System.out.println(("SEEK_EVALUATION: " + Math.floor(percentage * 10000) / 100) + "% finished");
    }

    @Override
    public void action(Properties results)
    {
        System.out.println("Finish.");
    }
}
