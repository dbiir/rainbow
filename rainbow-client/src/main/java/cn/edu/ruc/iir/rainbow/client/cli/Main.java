package cn.edu.ruc.iir.rainbow.client.cli;

import cn.edu.ruc.iir.rainbow.client.util.HttpSettings;
import cn.edu.ruc.iir.rainbow.client.util.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.client.cli
 * @ClassName: Main
 * @Description: To send workload by HTTP interface
 * @author: Tao
 * @date: Create in 2017-09-30 8:22
 **/
public class Main {

    public static void main(String args[]) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("Rainbow Client")
                .defaultHelp(true)
                .description("Upload workloads by giving piplineNo, queryid, weight and columns options.");
        parser.addArgument("-pno", "--piplineNo").required(true)
                .help("specify the pipline needed by pno");
        parser.addArgument("-i", "--queryID").required(true)
                .help("specify the queryid of the workload");
        parser.addArgument("-w", "--weight").required(true)
                .help("specify the weight of the workload");
        parser.addArgument("-c", "--columns").required(true)
                .help("specify the columns of the workload");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.out.println("Rainbow Client (https://github.com/dbiir/rainbow/blob/master/rainbow-client/README.md).");
            System.exit(0);
        }

        try {
            String pno = ns.getString("piplineNo");
            String id = ns.getString("queryID");
            String weight = ns.getString("weight");
            String query = ns.getString("columns");
            if (pno.length() > 0 && query.length() > 0 && weight.length() > 0 && id.length() > 0) {
//                Query q = new Query(pno, query, id);
//                String aPostData = JSON.toJSONString(q);
//                String res = HttpUtil.HttpPost(HttpSettings.WORKLOAD_POST_URL, aPostData).toString();
                String aPostData = "query=" + query + "&pno=" + pno + "&id=" + id + "&weight=" + weight;
                String res = HttpUtil.acHttpPost(HttpSettings.WORKLOAD_POST_URL, aPostData).toString();
                JSONObject j = JSON.parseObject(res);
                System.out.println(j.get("Res"));
            } else {
                System.out.println("Please input the piplineNo, queryID, weight & columns.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
