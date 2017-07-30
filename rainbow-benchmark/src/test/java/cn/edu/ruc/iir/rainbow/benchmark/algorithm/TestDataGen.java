package cn.edu.ruc.iir.rainbow.benchmark.algorithm;


import cn.edu.ruc.iir.rainbow.benchmark.domain.Column;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark
 * @ClassName: TestDataGen
 * @Description: To test the functions of the DataGen.java
 * @author: Tao
 * @date: Create in 2017-07-29 19:25
 **/
public class TestDataGen {

    @Test
    public void TestGetRandomIndex() {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int a = random.nextInt(10);
            System.out.print(a + " ");
        }
    }

    public String data_origin = "data/schema_origin.txt";

    @Test
    public void TestGetFileSize() {
        String filePath = this.getClass().getClassLoader()
                .getResource((data_origin)).getFile();
        File f = new File(filePath);
        DecimalFormat df = new DecimalFormat("#.00");
        String fileSizeString = "";
        if (f.exists() && f.isFile()) {
            long fileS = f.length();
            if (fileS < 1024) {
                fileSizeString = df.format((double) fileS) + "B";
            } else if (fileS < 1048576) {
                fileSizeString = df.format((double) fileS / 1024) + "K";
            } else if (fileS < 1073741824) {
                fileSizeString = df.format((double) fileS / 1048576) + "M";
            } else {
                fileSizeString = df.format((double) fileS / 1073741824) + "G";
            }
            System.out.println(fileSizeString);
        }
    }

    @Test
    public void TestCompareSize() {
        String size = "120.34";
        double d = Double.valueOf(size);
        if (d > 120) {
            System.out.println(d);
        }
        int i = 0;
        while (i++ < 4) {
            System.out.println(i);
        }
    }

    @Test
    public void TestGenDataBySize() {
        DataGen instance = DataGen.getInstance();
        long startTime = System.currentTimeMillis();
        int dataSize = 1;
        instance.genDataBySize(dataSize);
        long endTime = System.currentTimeMillis();
        System.out.println("dataSize*200M run time : ： " + (endTime - startTime) / 1000 + "s");
    }

    @Test
    public void TestGetValueByBinarySearch() {
        DataGen instance = DataGen.getInstance();
        Random random = new Random();
        List<Column> columnList = new ArrayList<>();
        Column c = new Column(3000, "Good");
        columnList.add(c);
        c = new Column(6000, "Job");
        columnList.add(c);
        c = new Column(10000, "You");
        columnList.add(c);
        c = new Column(20000, "Have");
        columnList.add(c);
        c = new Column(34000, "Done");
        columnList.add(c);
        c = new Column(38000, "Thank");
        columnList.add(c);
        c = new Column(40000, "You");
        columnList.add(c);
        for (int i = 0; i < 10; i++) {
            int randNum = random.nextInt(40000) + 1;
            String str = instance.getValueByBinarySearch(randNum, columnList);
            System.out.println(randNum + " -> " + str);
        }
        // test the boundary of the interval
        int randNum = 3000;
        String str1 = instance.getValueByBinarySearch(randNum, columnList);
        System.out.println(randNum + " -> " + str1);
    }


}
