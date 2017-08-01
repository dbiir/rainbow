package cn.edu.ruc.iir.rainbow.layout.sql;

import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.InputFactory;
import cn.edu.ruc.iir.rainbow.common.util.OutputFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

public class GenerateLoad
{
    private GenerateLoad () {}

    public static void LoadParq(boolean overWrite, String tableName, String schemaFilePath, String loadStatementPath) throws IOException
    {
        try (BufferedReader reader = InputFactory.Instance().getReader(schemaFilePath);
             BufferedWriter writer = OutputFactory.Instance().getWriter(loadStatementPath);)
        {
            String line;
            writer.write("INSERT " + (overWrite ? "OVERWRITE" : "INTO") + " TABLE " + tableName + "\nSELECT\n");
            line = reader.readLine();
            writer.write(line.split("\t")[0].toLowerCase());
            while ((line = reader.readLine()) != null)
            {
                writer.write(",\n" + line.split("\t")[0].toLowerCase());
            }
            writer.write("\nfrom " + ConfigFactory.Instance().getProperty("text.table.name") + "\n");
        }
    }

    public static void LoadOrc(boolean overWrite, String tableName, String schemaFilePath, String loadStatementPath) throws IOException
    {
        try (BufferedReader reader = InputFactory.Instance().getReader(schemaFilePath);
             BufferedWriter writer = OutputFactory.Instance().getWriter(loadStatementPath);)
        {
            String line;
            writer.write("INSERT " + (overWrite ? "OVERWRITE" : "INTO") + " TABLE " + tableName + "\nSELECT\n");
            line = reader.readLine();
            writer.write(line.split("\t")[0].toLowerCase());
            while ((line = reader.readLine()) != null)
            {
                writer.write(",\n" + line.split("\t")[0].toLowerCase());
            }
            writer.write("\nfrom " + ConfigFactory.Instance().getProperty("text.table.name") + "\n");
        }
    }
}
