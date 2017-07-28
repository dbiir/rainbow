package cn.edu.ruc.iir.rainbow.benchmark;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark
 * @ClassName: TestDictGenerator
 * @Description: To Test functions of Class DictGenerator
 * @author: Tao
 * @date: Create in 2017-07-28 8:54
 **/
public class TestDictGenerator {


    private DictGenerator dictGenerator = DictGenerator.Instance();

    @Test
    public void TestGetDataDict() {
        dictGenerator.getDataDict();
    }

    @Test
    public void TestFunctios() {
        Map<String, Integer> map = new HashMap<String, Integer>();
        String[] str = {"abc", "ab", "abc", "a", "b", "c", "ab"};
        int num = 1;
        for (String s : str) {
            if (map.get(s) != null) {
                num = map.get(s);
                num++;
            } else {
                num = 1;
            }
            map.put(s, num);
        }
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }

        for (String key : map.keySet()) {
            System.out.println("Key = " + key);
        }
        for (Integer value : map.values()) {
            System.out.println("Value = " + value);
        }
    }

    @Test
    public void TestGetDataDictByIndex() {
        long startTime = System.currentTimeMillis();
        int index = 0;
        dictGenerator.getDataDict(index);
        long endTime = System.currentTimeMillis();
        System.out.println("Program run time : ： " + (endTime - startTime) / 1000 + "ms");
    }

    /**
    * @ClassName: TestDictGenerator
    * @Title:
    * @Description: main method for the second task
    * @param:
    * @date: 0:03 2017/7/29
    */
    @Test
    public void TestGetDataDictByIndexAndColumnName() {
        DataGenerator dataGenerator = DataGenerator.Instance();
        String columnName[] = dataGenerator.getColumnName();
        long startTime = System.currentTimeMillis();
        for (int j = 0; j < columnName.length; j++) {
            long start = System.currentTimeMillis();
            dictGenerator.getDataDict(j, columnName);
            long end = System.currentTimeMillis();
            System.out.println(columnName[j] + " run time : ： " + (end - start) / 1000 + "s");
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Program run time : ： " + (endTime - startTime) / 1000 + "s");
    }

}
