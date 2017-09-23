package cn.edu.ruc.iir.rainbow.layout;

import org.junit.Test;

public class TestNaN
{
    @Test
    public void test ()
    {
        double a = Math.sqrt(-1);
        System.out.println(a);
        System.out.println(1.0+a);
        System.out.println(2.0*a);
        System.out.println(1>a);
        System.out.println(1<=a);
    }
}
