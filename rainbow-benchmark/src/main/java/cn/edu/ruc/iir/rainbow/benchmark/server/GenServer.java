package cn.edu.ruc.iir.rainbow.benchmark.server;

import cn.edu.ruc.iir.rainbow.benchmark.algorithm.DataGenerator;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark.server
 * @ClassName: GenServer
 * @Description: To start generating the datas by datasize
 * @author: Tao
 * @date: Create in 2017-07-31 8:21
 **/
public class GenServer {

    public static void main(String args[]) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("ruc.iir.rainbow.benchmark")
                .defaultHelp(true)
                .description("Generate data by given dataSize and threadNum.");
        parser.addArgument("-s", "--dataSize").setDefault("800")
                .help("Specify the size of data generated");
        parser.addArgument("-t", "--threadNum").setDefault("4")
                .help("Specify the number of threads to run");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        int dataSize = Integer.valueOf(ns.getString("dataSize"));
        int threadNum = Integer.valueOf(ns.getString("threadNum"));

        DataGenerator instance = DataGenerator.getInstance(threadNum);
        long startTime = System.currentTimeMillis();
        instance.genDataBySize(dataSize);
        long endTime = System.currentTimeMillis();
        System.out.println("dataSize: " + dataSize + "& threadNum: " + threadNum + " run time : ： " + (endTime - startTime) / 1000 + "s");
    }

}
