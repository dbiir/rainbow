package cn.edu.ruc.iir.rainbow.client;

import cn.edu.ruc.iir.rainbow.client.util.HttpSettings;
import cn.edu.ruc.iir.rainbow.client.util.HttpUtil;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.client
 * @ClassName: ClientTest
 * @Description:
 * @author: taoyouxian
 * @date: Create in 2017-10-10 17:25
 **/
public class ClientTest {

    String pno = "14ba30d7abdbe13ab2c886f18c0f5555";

    @Test
    public void APCTest() {
        String path = ConfigFactory.Instance().getProperty("pipline.path");
        String targetPath = path + "pipeline/" + pno;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(
                    path + "workload.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line = null;
        Random random = new Random(System.currentTimeMillis());
        try {
            int i = 0;
            while ((line = reader.readLine()) != null) {
                i++;
                String[] tokens = line.split("\t");
                double weight = Double.parseDouble(tokens[1]);
                String aPostData = "query=" + tokens[2] + "&pno=" + pno + "&id=" + tokens[0] + "&weight=" + weight;
                String res = HttpUtil.acHttpPost(HttpSettings.WORKLOAD_POST_URL, aPostData).toString();
                Thread.sleep(random.nextInt(500));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
