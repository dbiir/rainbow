package cn.edu.ruc.iir.rainbow.common;

import cn.edu.ruc.iir.rainbow.common.workload.AccessPattern;
import cn.edu.ruc.iir.rainbow.common.workload.AccessPatternCache;
import org.apache.commons.httpclient.util.DateUtil;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Random;

public class TestAPC
{
    @Test
    public void test () throws IOException, InterruptedException
    {
        BufferedReader reader = new BufferedReader(new FileReader(
                "H:\\SelfLearning\\SAI\\DBIIR\\rainbows\\workload.txt"));
        String line = null;
        int i = 0, j = 0;
        Random random = new Random(System.currentTimeMillis());
        AccessPatternCache APC = new AccessPatternCache(100000, 0.1);
        System.out.println(DateUtil.formatDate(new Date()));
        while ((line = reader.readLine()) != null)
        {
            i++;
            String[] tokens = line.split("\t");
            double weight = Double.parseDouble(tokens[1]);
            AccessPattern pattern = new AccessPattern(tokens[0], weight);
            for (String column : tokens[2].split(","))
            {
                pattern.addColumn(column);
            }

            if (APC.cache(pattern))
            {
                System.out.println(DateUtil.formatDate(new Date()));
                System.out.println(i + ", trigger layout optimization.");
                j++;
                APC.saveAsWorkloadFile("H:\\SelfLearning\\SAI\\DBIIR\\rainbows\\workload_"+j+".txt");
                System.out.println(DateUtil.formatDate(new Date()));
            }
            Thread.sleep(random.nextInt(500));
        }
    }
}
