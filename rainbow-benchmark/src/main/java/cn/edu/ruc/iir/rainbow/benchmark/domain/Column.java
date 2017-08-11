package cn.edu.ruc.iir.rainbow.benchmark.domain;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark.domain
 * @ClassName: Column
 * @Description: To contain the value & the upper bound of each content of each column
 * @author: Tao
 * @date: Create in 2017-07-29 21:15
 **/
public class Column
{
    private int upperBound;
    private String value;

    public Column()
    {
    }

    public Column(int upperBound, String value)
    {
        this.upperBound = upperBound;
        this.value = value;
    }

    public int getUpperBound()
    {
        return upperBound;
    }

    public void setUpperBound(int upperBound)
    {
        this.upperBound = upperBound;
    }

    public String getValue()
    {
        return value;
    }

    public void setValue(String value)
    {
        this.value = value;
    }
}
