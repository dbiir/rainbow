package cn.edu.ruc.iir.rainbow.layout;

import cn.edu.ruc.iir.rainbow.layout.seekcost.SeekCostFunction;
import org.junit.Test;

public class TestEnum
{
    @Test
    public void testEnumName ()
    {
        System.out.println(SeekCostFunction.Type.POWER.toString());
        System.out.println(SeekCostFunction.Type.POWER.name());
    }
}
