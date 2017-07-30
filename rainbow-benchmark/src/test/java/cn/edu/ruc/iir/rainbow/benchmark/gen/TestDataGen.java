package cn.edu.ruc.iir.rainbow.benchmark.gen;

import cn.edu.ruc.iir.rainbow.benchmark.domain.ColumnArray;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark.gen
 * @ClassName: TestDataGen
 * @Description: To test functions of DataGen.java
 * @author: Tao
 * @date: Create in 2017-07-30 17:03
 **/
public class TestDataGen
{
    @Test
    public void TestGetValueByArray() {
        Random random = new Random();
        List<ColumnArray> columnList = new ArrayList<>();
        ColumnArray c = new ColumnArray();
        try {
            for (int i = 0; i < 3000; i++)
                c.getArray()[i] = "Good";
            for (int i = 3000; i < 10000; i++)
                c.getArray()[i] = "Job";
            for (int i = 10000; i < 25000; i++)
                c.getArray()[i] = "You";
            for (int i = 25000; i < 30000; i++)
                c.getArray()[i] = "Have";
            for (int i = 30000; i < 35000; i++)
                c.getArray()[i] = "Done";
            for (int i = 35000; i < 40000; i++)
                c.getArray()[i] = "!";
            columnList.add(c);
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 10; i++) {
            int randNum = random.nextInt(40000) + 1;
            String str = columnList.get(0).getArray()[randNum];
            System.out.println(randNum + " -> " + str);
        }
    }

    @Test
    public void TestGenDataBySize() {
        DataGen instance = new DataGen(4);
        long startTime = System.currentTimeMillis();
        int dataSize = 2000;
        instance.genDataBySize(dataSize);
        long endTime = System.currentTimeMillis();
        System.out.println("dataSize * 200M run time : ： " + (endTime - startTime) / 1000 + "s");
    }

}
