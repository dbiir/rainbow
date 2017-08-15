package cn.edu.ruc.iir.rainbow.core.cli;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.core.invoker.InvokerFactory;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.Scanner;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.core.cli
 * @ClassName: Main
 * @Description: To start invoker
 * @author: Tao
 * @date: Create in 2017-08-14 23:26
 **/
public class Main
{
    public static void main(String args[])
    {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("ruc.iir.rainbow.core")
                .defaultHelp(true)
                .description("Rainbow: Data Layout Optimization framework for very wide tables on HDFS.");
        parser.addArgument("-f", "--config")
                .help("specify the path of configuration file");
        parser.addArgument("-d", "--params_dir").required(true)
                .help("specify the directory of parameter files");
        Namespace namespace = null;
        try
        {
            namespace = parser.parseArgs(args);
        } catch (ArgumentParserException e)
        {
            parser.handleError(e);
            System.out.println("Rainbow Core (https://github.com/dbiir/rainbow/tree/master/rainbow-core).");
            System.exit(1);
        }

        try
        {
            String configFilePath = namespace.getString("config");
            Scanner scanner = new Scanner(System.in);
            String inputStr;

            if (configFilePath != null)
            {
                ConfigFactory.Instance().LoadProperties(configFilePath);
                System.out.println("System settings loaded from " + configFilePath + ".");
            } else
            {
                System.out.println("Using default system settings.");
            }

            String paramsDirPath = namespace.getString("params_dir");
            if (! paramsDirPath.endsWith("/"))
            {
                paramsDirPath += "/";
            }
            File tmpFile = new File(paramsDirPath);
            if ((!tmpFile.exists()) || (!tmpFile.isDirectory()))
            {
                System.out.println("Invalid parameter file directory.");
                System.exit(1);
            }
            System.out.println("Using parameters in " + paramsDirPath + ".");

            while (true)
            {
                System.out.print("rainbow> ");
                inputStr = scanner.nextLine().trim();

                if (inputStr.isEmpty() || inputStr.equals(";"))
                {
                    continue;
                }

                if (inputStr.endsWith(";"))
                {
                    inputStr = inputStr.substring(0, inputStr.length()-1);
                }

                if (inputStr.equalsIgnoreCase("exit"))
                {
                    System.out.println("Bye.");
                    break;
                }

                if (inputStr.equalsIgnoreCase("help"))
                {
                    System.out.println("Get supported commands from https://github.com/dbiir/rainbow/tree/master/rainbow-core");
                    continue;
                }

                String command = inputStr.trim().split("\\s+")[0].toUpperCase();

                try
                {
                    Invoker invoker = InvokerFactory.Instance().getInvoker(command);
                    Properties params = new Properties();
                    params.load(new FileInputStream(paramsDirPath + command + ".properties"));

                    if (command.equals("REDIRECT"))
                    {
                        String columns = inputStr.trim().split("\\s+")[1];
                        params.setProperty("column.set", columns);
                    }

                    if (command.equalsIgnoreCase("SEEK_EVALUATION"))
                    {
                        String distance = inputStr.trim().split("\\s+")[1];
                        params.setProperty("seek.distance", ""+Integer.parseInt(distance));
                    }
                    System.out.println("Executing command: " + command);
                    //invoker.executeCommands(params);
                } catch (IllegalArgumentException e)
                {
                    System.out.println("Illegal Command: " + command);
                }
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
