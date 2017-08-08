package cn.edu.ruc.iir.rainbow.eva;

import org.junit.Test;

import java.io.File;

public class TestFileLength
{
    @Test
    public void test ()
    {
        File file = new File(TestFileLength.class.getResource("/drop_caches.sh").getFile());
        System.out.println(file.length());
    }
}
