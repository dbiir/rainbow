package cn.edu.ruc.iir.rainbow.core.cli;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.core.invoker.*;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
public class Main {

    static Scanner sc = new Scanner(System.in);

    /**
     * params given should contain the following settings:
     * <ol>
     * <li>-s (sys_settings)</li>
     * </ol>
     * <p>
     * command followed should contain the following settings:
     * <ol>
     * <li>-t (invoker_type)</li>
     * <li>-d (params_directory)</li>
     * </ol>
     *
     * @param args
     */
    public static void main(String args[]) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("ruc.iir.rainbow.core")
                .defaultHelp(true)
                .description("Give invoker with parameter options.");
        parser.addArgument("-s", "--sys_settings")
                .help("specify the settings of system");
        parser.addArgument("-t", "--invoker_type")
                .choices("GENERATE_SQL", "ORDERING", "DUPLICATION", "QUERY")
                .help("specify which invoker to run");
        parser.addArgument("-d", "--params_directory")
                .help("specify the directory of parameters");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.out.println("rainbow> Rainbow Core (https://github.com/dbiir/rainbow/tree/master/rainbow-core).");
            System.exit(1);
        }

        String inputStr = "";
        String curCommand = "";
        try {
            String sys_settings = ns.getString("sys_settings");
            if (sys_settings != null) {
                ConfigFactory configFactory = ConfigFactory.Instance();
                configFactory.LoadProperties(sys_settings);
                System.out.println("rainbow> sys_settings loaded, please input -t & -d.");
                while (!inputStr.equals("exit;")) {
                    System.out.print("rainbow> ");
                    inputStr = sc.nextLine();
                    if (!inputStr.endsWith(";")) {
                        inputStr += ";";
                    }
                    if (inputStr.equals("exit;")) {
                        break;
                    }
                    int left = inputStr.indexOf("-");
                    int right = inputStr.lastIndexOf(";");
                    curCommand = inputStr.substring(left, right);
                    ns = getCommandParamsByArgs(curCommand, parser);
                    // run the invoker
                    excuteCommandByInvoker(ns);
                }
            } else {
                System.out.println("rainbow> Please input the '-s sys_settings'.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void excuteCommandByInvoker(Namespace ns) {
        String invoker_type = ns.getString("invoker_type");
        String params_directory = ns.getString("params_directory");
        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(params_directory);
            prop.load(in);
            Invoker invoker = getInvoker(invoker_type.toUpperCase());
            if (invoker == null) {
                System.out.print("invoker not exit, input command again.");
            } else {
                try {
                    invoker.executeCommands(prop);
                } catch (InvokerException e) {
                    e.printStackTrace();
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static Invoker getInvoker(String invokerName) {
        switch (invokerName) {
            case "GENERATE_SQL":
                return new InvokerGenerateSQL();
            case "ORDERING":
                return new InvokerOrdering();
            case "DUPLICATION":
                return new InvokerDuplication();
            case "QUERY":
                return new InvokerQuery();
            default:
                return null;
        }
    }

    private static Namespace getCommandParamsByArgs(String curCommand, ArgumentParser parser) {
        String[] args = curCommand.split(" ");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            e.printStackTrace();
        }
        return ns;
    }
}
