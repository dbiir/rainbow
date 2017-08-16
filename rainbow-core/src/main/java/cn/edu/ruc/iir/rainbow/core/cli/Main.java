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
        ArgumentParser parser = ArgumentParsers.newArgumentParser("Rainbow Core")
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
                        ArgumentParser parser1 = ArgumentParsers.newArgumentParser("REDIRECT")
                                .defaultHelp(true);
                        parser1.addArgument("-s", "--column_set").required(true)
                                .help("specify the set of columns to redirect, separated by comma");
                        Namespace namespace1;
                        try
                        {
                            namespace1 = parser1.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                        } catch (ArgumentParserException e)
                        {
                            parser1.handleError(e);
                            continue;
                        }
                        params.setProperty("column.set", namespace1.getString("column_set"));
                    }

                    if (command.equals("SEEK_EVALUATION"))
                    {
                        ArgumentParser parser1 = ArgumentParsers.newArgumentParser("SEEK_EVALUATION")
                                .defaultHelp(true);
                        parser1.addArgument("-d", "--distance").required(true)
                                .help("specify the distance of bytes to seek");
                        Namespace namespace1;
                        try
                        {
                            namespace1 = parser1.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                        } catch (ArgumentParserException e)
                        {
                            parser1.handleError(e);
                            continue;
                        }
                        params.setProperty("seek.distance", namespace1.getString("distance"));
                    }

                    if (command.equals("GENERATE_DDL"))
                    {
                        ArgumentParser parser1 = ArgumentParsers.newArgumentParser("GENERATE_DDL")
                                .defaultHelp(true);
                        parser1.addArgument("-f", "--format").required(true)
                                .help("specify the file format, can be TEXT, PARUQET or ORC");
                        parser1.addArgument("-s", "--schema_file").required(true)
                                .help("specify the path of schema file");
                        parser1.addArgument("-d", "--ddl_file").required(true)
                                .help("specify the path of ddl file");
                        parser1.addArgument("-t", "--table_name")
                                .help("specify name of the table if file format is not TEXT");
                        Namespace namespace1;
                        try
                        {
                            namespace1 = parser1.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                        } catch (ArgumentParserException e)
                        {
                            parser1.handleError(e);
                            continue;
                        }
                        params.setProperty("file.format", namespace1.getString("format"));
                        params.setProperty("schema.file", namespace1.getString("schema_file"));
                        params.setProperty("ddl.file", namespace1.getString("ddl_file"));
                        if (! params.getProperty("file.format").equals("TEXT"))
                        {
                            if (namespace1.getString("table_name") == null)
                            {
                                System.out.println("table name is not given");
                                continue;
                            }
                            params.setProperty("table.name", namespace1.getString("table_name"));
                        }
                    }

                    if (command.equals("GENERATE_LOAD"))
                    {
                        ArgumentParser parser1 = ArgumentParsers.newArgumentParser("GENERATE_LOAD")
                                .defaultHelp(true);
                        parser1.addArgument("-r", "--overwrite").required(true)
                                .help("specify whether or not to overwrite data in the table");
                        parser1.addArgument("-s", "--schema_file").required(true)
                                .help("specify the path of schema file");
                        parser1.addArgument("-l", "--load_file").required(true)
                                .help("specify the path of load file");
                        parser1.addArgument("-t", "--table_name").required(true)
                                .help("specify name of the table to be loaded");
                        Namespace namespace1;
                        try
                        {
                            namespace1 = parser1.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                        } catch (ArgumentParserException e)
                        {
                            parser1.handleError(e);
                            continue;
                        }
                        params.setProperty("overwrite", namespace1.getString("overwrite"));
                        params.setProperty("schema.file", namespace1.getString("schema_file"));
                        params.setProperty("load.file", namespace1.getString("load_file"));
                        params.setProperty("table.name", namespace1.getString("table_name"));
                    }

                    if (command.equals("GET_COLUMN_SIZE"))
                    {
                        ArgumentParser parser1 = ArgumentParsers.newArgumentParser("GET_COLUMN_SIZE")
                                .defaultHelp(true);
                        parser1.addArgument("-f", "--format").required(true)
                                .help("specify the file format, can be PARUQET or ORC");
                        parser1.addArgument("-s", "--schema_file").required(true)
                                .help("specify the path of schema file");
                        parser1.addArgument("-p", "--hdfs_table_path").required(true)
                                .help("specify the path of load file");
                        Namespace namespace1;
                        try
                        {
                            namespace1 = parser1.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                        } catch (ArgumentParserException e)
                        {
                            parser1.handleError(e);
                            continue;
                        }
                        params.setProperty("file.format", namespace1.getString("format"));
                        params.setProperty("schema.file", namespace1.getString("schema_file"));
                        params.setProperty("hdfs.table.path", namespace1.getString("hdfs_table_path"));
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
