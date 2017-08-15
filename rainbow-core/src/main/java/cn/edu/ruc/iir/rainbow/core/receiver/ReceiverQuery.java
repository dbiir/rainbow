package cn.edu.ruc.iir.rainbow.core.receiver;

import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;

import java.util.Properties;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.core.receiver
 * @ClassName: ReceiverQuery
 * @Description: receiver query
 * @author: Tao
 * @date: Create in 2017-08-13 18:16
 **/
public class ReceiverQuery implements Receiver
{
    @Override
    public void progress(double percentage)
    {
        System.out.println(("Query: " + Math.floor(percentage * 10000) / 100) + "% finished");
    }

    @Override
    public void action(Properties results)
    {
        System.out.println("finish.");
    }
}
