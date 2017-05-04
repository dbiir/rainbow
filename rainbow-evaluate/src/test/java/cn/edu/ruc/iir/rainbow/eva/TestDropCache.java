package cn.edu.ruc.iir.rainbow.eva;

import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Created by hank on 2015/2/27.
 */
public class TestDropCache
{
    @Test
    void testDropCache () throws IOException
    {
        // success!
        Runtime.getRuntime().exec("/root/drop_caches.sh");
    }
}
