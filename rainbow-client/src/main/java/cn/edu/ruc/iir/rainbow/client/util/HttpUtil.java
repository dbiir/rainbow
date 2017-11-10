package cn.edu.ruc.iir.rainbow.client.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.client.util
 * @ClassName: HttpUtil
 * @Description: deal with post & get request
 * @author: Tao
 * @date: Create in 2017-09-30 8:49
 **/
public class HttpUtil {

    public static Object HttpPost(String aUrl, String aPostData) {
        String res = "";
        DefaultHttpClient httpClient = new DefaultHttpClient();
        HttpPost httpost = new HttpPost(aUrl);
        try {
            httpost.setEntity(new StringEntity(aPostData, "UTF-8"));
            HttpResponse response = httpClient.execute(httpost);
            res = EntityUtils.toString(response.getEntity(), "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    public static Object acHttpPost(String aUrl, String param) {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(aUrl);
            URLConnection conn = realUrl.openConnection();
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            out = new PrintWriter(conn.getOutputStream());
            out.print(param);
            out.flush();
            in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("POST error！" + e);
            e.printStackTrace();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return result;
    }

    public static Object HttpGet(String aUrl) {
        String res = "";
        HttpClient httpClient = new DefaultHttpClient();
        HttpGet httpGet = new HttpGet(aUrl);// init
        try {
            HttpResponse httpResponse = httpClient.execute(httpGet);// accept msg
            HttpEntity entity = httpResponse.getEntity();// get result from msg
            if (entity != null) {
                res = EntityUtils.toString(entity, "UTF-8");// change to string type
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

}
