package cn.edu.ruc.iir.rainbow.benchmark.cli;

import cn.edu.ruc.iir.rainbow.benchmark.generator.DataGenerator;
import cn.edu.ruc.iir.rainbow.benchmark.util.SysSettings;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark.cli
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
        parser.addArgument("-s", "--dataSize")
                .help("Specify the size of data generated");
        parser.addArgument("-t", "-threadNum").setDefault("4")
                .help("Specify the number of threads to run");
        parser.addArgument("-d", "--directory")
                .help("Specify the directory of config-info to read");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.out.println("Please input the command 'java -jar rainbow-benchmark-1.0-SNAPSHOT-dbiir.jar -h'.");
            System.exit(1);
        }

        try {
            int dataSize = Integer.valueOf(ns.getString("dataSize"));
            int threadNum = Integer.valueOf(ns.getString("threadNum"));
            String directory = ns.getString("directory");
            if (dataSize > 0 && threadNum > 0 && directory != null) {
                SysSettings.CONFIG_DIRECTORY = directory;
                DataGenerator instance = DataGenerator.getInstance(threadNum);
                long startTime = System.currentTimeMillis();
                instance.genDataBySize(dataSize);
                long endTime = System.currentTimeMillis();
                System.out.println("dataSize: " + dataSize + "MB & threadNum: " + threadNum + " -- run time : ： " + (endTime - startTime) / 1000 + "s");
            } else {
                System.out.println("Please input the dataSize & threadNum & directory.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
