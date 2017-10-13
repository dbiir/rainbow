package cn.edu.ruc.iir.rainbow.client;

import cn.edu.ruc.iir.rainbow.client.util.HttpSettings;
import cn.edu.ruc.iir.rainbow.client.util.HttpUtil;
import cn.edu.ruc.iir.rainbow.web.hdfs.common.SysConfig;
import org.junit.Test;

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

    @Test
    public void UploadTest() {
        try {
            String pno = "";
            String id = "";
            String weight = "";
            String query = "";
            System.out.println("Please input the piplineNo, queryID, weight & columns.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    String pno = "44ba30d7abdbe13ab2c886f18c0f5556";

    @Test
    public void APCTest() {
        String targetPath = SysConfig.Catalog_Project + "pipeline/" + pno;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(
                    SysConfig.Catalog_Project + "/workload.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line = null;
        Random random = new Random(System.currentTimeMillis());
        try {
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("\t");
                double weight = Double.parseDouble(tokens[1]);
                String aPostData = "query=" + tokens[2] + "&pno=" + pno + "&id=" + tokens[0] + "&weight=" + weight;
                String res = HttpUtil.acHttpPost(HttpSettings.WORKLOAD_POST_URL, aPostData).toString();
                Thread.sleep(random.nextInt(2000));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
