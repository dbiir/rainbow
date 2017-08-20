package cn.edu.ruc.iir.rainbow.seek;

import org.junit.Test;

import java.io.File;

public class TestPath
{
    @Test
    public void test ()
    {
        File dir = new File("/home/hank/");
        System.out.println(dir.getPath());
    }
}
