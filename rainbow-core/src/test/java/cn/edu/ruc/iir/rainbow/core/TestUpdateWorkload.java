package cn.edu.ruc.iir.rainbow.core;

import cn.edu.ruc.iir.rainbow.common.util.InputFactory;
import cn.edu.ruc.iir.rainbow.common.util.OutputFactory;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.core
 * @ClassName: TestUpdateWorkload
 * @Description: update workload
 * @author: Tao
 * @date: Create in 2017-08-13 19:42
 **/
public class TestUpdateWorkload {


    @Test
    public void test() {
        try (BufferedReader br = InputFactory.Instance().getReader("G:\\rainbow-benchmark\\data\\workload_dupped.txt");
             BufferedWriter bw = OutputFactory.Instance().getWriter("G:\\DBIIR\\rainbow\\rainbow-core\\src\\test\\java\\dataset\\workload_dupped.txt");) {
            String curLine;
            String splitLine[];
            while ((curLine = br.readLine()) != null) {
                splitLine = curLine.split("\t");
                bw.write(splitLine[0] + "\t1\t" + splitLine[1] + "\n");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
