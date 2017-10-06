package cn.edu.ruc.iir.rainbow.common;

import cn.edu.ruc.iir.rainbow.common.workload.AccessPattern;
import cn.edu.ruc.iir.rainbow.common.workload.AccessPatternCache;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

public class TestAPC
{
    @Test
    public void test () throws IOException, InterruptedException
    {
        BufferedReader reader = new BufferedReader(new FileReader(
                "/home/hank/dev/idea-projects/rainbow/rainbow-layout/src/test/resources/workload.txt"));
        String line = null;
        int i = 0, j = 0;
        Random random = new Random(System.currentTimeMillis());
        AccessPatternCache APC = new AccessPatternCache(4000, 0.1);
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
                System.out.println(i + ", trigger layout optimization.");
                j++;
                APC.saveAsWorkloadFile("/home/hank/Desktop/workload_"+j+".txt");
            }
            Thread.sleep(random.nextInt(20));
        }
    }
}
