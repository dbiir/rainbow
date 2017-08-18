package cn.edu.ruc.iir.rainbow.core.cli;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.core.invoker.InvokerFactory;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

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
        //parser.addArgument("-d", "--params_dir").required(true)
        //       .help("specify the directory of parameter files");
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

            /*
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
            */

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

                if (inputStr.equalsIgnoreCase("exit") || inputStr.equalsIgnoreCase("quit") ||
                        inputStr.equalsIgnoreCase("-q"))
                {
                    System.out.println("Bye.");
                    break;
                }

                if (inputStr.equalsIgnoreCase("help") || inputStr.equalsIgnoreCase("-h"))
                {
                    System.out.println("Supported commands:\n" +
                            "GENERATE_DDL\n" +
                            "GENERATE_LOAD\n" +
                            "ORDERING\n" +
                            "DUPLICATION\n" +
                            "BUILD_INDEX\n" +
                            "REDIRECT\n" +
                            "GENERATE_QUERY\n");
                    System.out.println("{command} -h to show the usage of a command.\nexit / quit / -q to exit.\n");
                    System.out.println("Examples on using these commands: https://github.com/dbiir/rainbow/tree/master/rainbow-core");
                    continue;
                }

                String command = inputStr.trim().split("\\s+")[0].toUpperCase();

                try
                {
                    Invoker invoker = InvokerFactory.Instance().getInvoker(command);
                    Properties params = new Properties();
                    //params.load(new FileInputStream(paramsDirPath + command + ".properties"));

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

                    if (command.equals("ORDERING"))
                    {
                        ArgumentParser parser1 = ArgumentParsers.newArgumentParser("ORDERING")
                                .defaultHelp(true);
                        parser1.addArgument("-a", "--algorithm")
                                .help("specify the ordering algorithm, can be SCOA or AUTOPART, default is SCOA");
                        parser1.addArgument("-s", "--schema_file").required(true)
                                .help("specify the path of schema file");
                        parser1.addArgument("-o", "--ordered_schema_file").required(true)
                                .help("specify the path of ordered schema file, this is the ordering result");
                        parser1.addArgument("-w", "--workload_file").required(true)
                                .help("specify the path of workload file");
                        parser1.addArgument("-f", "--seek_cost_function")
                                .help("specify the seek cost function, can be POWER, LINEAR or SIMULATED, default is POWER");
                        parser1.addArgument("-p", "--seek_cost_file")
                                .help("specify the path of seek cost file");
                        parser1.addArgument("-b", "--budget")
                                .help("specify the computation budget in seconds, default is 200s");
                        Namespace namespace1;
                        try
                        {
                            namespace1 = parser1.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));

                        } catch (ArgumentParserException e)
                        {
                            parser1.handleError(e);
                            continue;
                        }
                        if (namespace1.getString("algorithm") != null)
                        {
                            params.setProperty("algorithm.name", namespace1.getString("algorithm"));
                        }
                        params.setProperty("schema.file", namespace1.getString("schema_file"));
                        params.setProperty("ordered.schema.file", namespace1.getString("ordered_schema_file"));
                        params.setProperty("workload.file", namespace1.getString("workload_file"));
                        if (namespace1.getString("seek_cost_function") != null)
                        {
                            params.setProperty("seek.cost.function", namespace1.getString("seek_cost_function"));
                        }

                        if ( params.getProperty("seek.cost.function").equals("SIMULATED"))
                        {
                            if (namespace1.getString("seek_cost_file") == null)
                            {
                                System.out.println("seek cost file is not given");
                                continue;
                            }
                            params.setProperty("seek.cost.file", namespace1.getString("seek_cost_file"));
                        }

                        if (namespace1.getString("budget") != null)
                        {
                            params.setProperty("computation.budget", namespace1.getString("budget"));
                        }
                    }

                    if (command.equals("DUPLICATION"))
                    {
                        ArgumentParser parser1 = ArgumentParsers.newArgumentParser("DUPLICATION")
                                .defaultHelp(true);
                        parser1.addArgument("-a", "--algorithm")
                                .help("specify the ordering algorithm, can be INSERTION or GRAVITY, default is INSERTION");
                        parser1.addArgument("-s", "--schema_file").required(true)
                                .help("specify the path of schema file");
                        parser1.addArgument("-ds", "--dupped_schema_file").required(true)
                                .help("specify the path of duplicated schema file, this is the duplication result");
                        parser1.addArgument("-w", "--workload_file").required(true)
                                .help("specify the path of workload file");
                        parser1.addArgument("-dw", "--dupped_workload_file").required(true)
                                .help("specify the path of duplicated workload file, this is the duplication result");
                        parser1.addArgument("-f", "--seek_cost_function")
                                .help("specify the seek cost function, can be POWER, LINEAR or SIMULATED, default is POWER");
                        parser1.addArgument("-p", "--seek_cost_file")
                                .help("specify the path of seek cost file");
                        parser1.addArgument("-b", "--budget")
                                .help("specify the computation budget in seconds, default is 3000s");
                        Namespace namespace1;
                        try
                        {
                            namespace1 = parser1.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));

                        } catch (ArgumentParserException e)
                        {
                            parser1.handleError(e);
                            continue;
                        }
                        if (namespace1.getString("algorithm") != null)
                        {
                            params.setProperty("algorithm.name", namespace1.getString("algorithm"));
                        }
                        params.setProperty("schema.file", namespace1.getString("schema_file"));
                        params.setProperty("dupped.schema.file", namespace1.getString("dupped_schema_file"));
                        params.setProperty("workload.file", namespace1.getString("workload_file"));
                        params.setProperty("dupped.workload.file", namespace1.getString("dupped_workload_file"));
                        if (namespace1.getString("seek_cost_function") != null)
                        {
                            params.setProperty("seek.cost.function", namespace1.getString("seek_cost_function"));
                        }

                        if ( params.getProperty("seek.cost.function").equals("SIMULATED"))
                        {
                            if (namespace1.getString("seek_cost_file") == null)
                            {
                                System.out.println("seek cost file is not given");
                                continue;
                            }
                            params.setProperty("seek.cost.file", namespace1.getString("seek_cost_file"));
                        }

                        if (namespace1.getString("budget") != null)
                        {
                            params.setProperty("computation.budget", namespace1.getString("budget"));
                        }
                    }

                    if (command.equals("BUILD_INDEX"))
                    {
                        ArgumentParser parser1 = ArgumentParsers.newArgumentParser("BUILD_INDEX")
                                .defaultHelp(true);
                        parser1.addArgument("-ds", "--dupped_schema_file").required(true)
                                .help("specify the path of duplicated schema file, this is the duplication result");
                        parser1.addArgument("-dw", "--dupped_workload_file").required(true)
                                .help("specify the path of duplicated workload file, this is the duplication result");
                        Namespace namespace1;
                        try
                        {
                            namespace1 = parser1.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));

                        } catch (ArgumentParserException e)
                        {
                            parser1.handleError(e);
                            continue;
                        }
                        params.setProperty("dupped.schema.file", namespace1.getString("dupped_schema_file"));
                        params.setProperty("workload.file", namespace1.getString("workload_file"));
                    }

                    if (command.equals("REDIRECT"))
                    {
                        ArgumentParser parser1 = ArgumentParsers.newArgumentParser("REDIRECT")
                                .defaultHelp(true);
                        parser1.addArgument("-q", "--query_id")
                                .help("specify the identifier of the query to be redirected");
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
                        if (namespace1.getString("query_id") != null)
                        {
                            params.setProperty("query.id",  namespace1.getString("query_id"));
                        }
                    }

                    if (command.equals("GENERATE_QUERY"))
                    {
                        ArgumentParser parser1 = ArgumentParsers.newArgumentParser("GENERATE_QUERY")
                                .defaultHelp(true);
                        parser1.addArgument("-t", "--table_name").required(true)
                                .help("specify name of the table used in query");
                        parser1.addArgument("-s", "--schema_file").required(true)
                                .help("specify the path of schema file");
                        parser1.addArgument("-w", "--workload_file").required(true)
                                .help("specify the path of workload file");
                        parser1.addArgument("-n", "--namenode").required(true)
                                .help("specify name of the table to be loaded");
                        parser1.addArgument("-sq", "--spark_query_file").required(true)
                                .help("specify the path of spark query file, which contains the generated query for spark cli");
                        parser1.addArgument("-hq", "--hive_query_file").required(true)
                                .help("specify the path of Hive query file, which contains the generated HiveQL");
                        Namespace namespace1;
                        try
                        {
                            namespace1 = parser1.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                        } catch (ArgumentParserException e)
                        {
                            parser1.handleError(e);
                            continue;
                        }
                        params.setProperty("namenode", namespace1.getString("namenode"));
                        params.setProperty("schema.file", namespace1.getString("schema_file"));
                        params.setProperty("workload.file", namespace1.getString("workload_file"));
                        params.setProperty("table.name", namespace1.getString("table_name"));
                        params.setProperty("spark.query.file", namespace1.getString("spark_query_file"));
                        params.setProperty("hive.query.file", namespace1.getString("hive_query_file"));
                    }

                    System.out.println("Executing command: " + command);
                    invoker.executeCommands(params);
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
