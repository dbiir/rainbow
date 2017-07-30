package cn.edu.ruc.iir.rainbow.benchmark.domain;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark.domain
 * @ClassName: ColumnArray
 * @Description: To contain the value & the array that contains each content of each column
 * @author: Tao
 * @date: Create in 2017-07-29 21:15
 **/
public class ColumnArray {

    private String[] array;

    public ColumnArray() {
        array = new String[40000];
    }

    public ColumnArray(String[] array) {
        this.array = array;
    }

    public String[] getArray() {
        return array;
    }

    public void setArray(String[] array) {
        this.array = array;
    }

}
