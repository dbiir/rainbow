package cn.edu.ruc.iir.rainbow.client.cli;

import cn.edu.ruc.iir.rainbow.client.util.HttpSettings;
import cn.edu.ruc.iir.rainbow.client.util.HttpUtil;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.client.cli
 * @ClassName: Client
 * @Description: To send workload regularly by HTTP interface according to the time
 * @author: taoyouxian
 * @date: Create in 2017-10-16 9:04
 **/
public class Client {

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("Rainbow Client")
                .defaultHelp(true)
                .description("Upload workloads by giving piplineNo and directory options.");
        parser.addArgument("-pno", "--piplineNo").required(true)
                .help("specify the pipline needed by pno");
        parser.addArgument("-d", "--directory").required(true)
                .help("specify the directory of workload file template");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.out.println("Rainbow Client (https://github.com/dbiir/rainbow/blob/master/rainbow-client/README.md).");
            System.exit(0);
        }

        String pno = ns.getString("piplineNo");
        String directory = ns.getString("directory");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(
                    directory));
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
                Thread.sleep(random.nextInt(500));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
