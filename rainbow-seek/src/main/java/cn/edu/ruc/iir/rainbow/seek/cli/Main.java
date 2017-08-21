package cn.edu.ruc.iir.rainbow.seek.cli;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.seek.invoker.InvokerGenerateFile;
import cn.edu.ruc.iir.rainbow.seek.invoker.InvokerSeekEvaluation;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.FileInputStream;
import java.util.Properties;

public class Main
{
    public static void main(String[] args)
    {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("Rainbow Seek Evaluation")
                .defaultHelp(true)
                .description("Evaluate seek cost of HDD-based file system.");
        parser.addArgument("-c", "--command").required(true)
                .help("specify the command, can be SEEK_EVALUATION or GENERATE_FILE");
        parser.addArgument("-p", "--param_file").required(true)
                .help("specify the path of parameter file");

        Namespace namespace = null;
        try
        {
            namespace = parser.parseArgs(args);
        } catch (ArgumentParserException e)
        {
            parser.handleError(e);
            System.out.println("Rainbow Seek Evaluation (https://github.com/dbiir/rainbow/blob/master/rainbow-seek/README.md).");
            System.exit(0);
        }

        try
        {
            String command = namespace.getString("command");
            String paramFilePath = namespace.getString("param_file");
            Invoker invoker;
            if (command.equalsIgnoreCase("GENERATE_FILE"))
            {
                invoker = new InvokerGenerateFile();
            }
            else if (command.equalsIgnoreCase("SEEK_EVALUATION"))
            {
                invoker = new InvokerSeekEvaluation();
            }
            else
            {
                System.out.println("Illegal Command: " + command);
                return;
            }
            Properties params = new Properties();
            params.load(new FileInputStream(paramFilePath));
            System.out.println("Executing command: " + command);
            invoker.executeCommands(params);
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
