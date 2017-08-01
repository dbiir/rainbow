package cn.edu.ruc.iir.rainbow.layout.sql;

import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.InputFactory;
import cn.edu.ruc.iir.rainbow.common.util.OutputFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

public class GenerateDDL
{
	private GenerateDDL() {}

	private static final String dataDir = ConfigFactory.Instance().getProperty("data.dir");

	public static void GenCreateOrc (String tableName, String schemaFilePath, String createStatementPath) throws IOException
	{
		try (BufferedReader reader = InputFactory.Instance().getReader(schemaFilePath);
			 BufferedWriter writer = OutputFactory.Instance().getWriter(createStatementPath))
		{
			String line;
			writer.write("create external table " + tableName + "\n(\n");
			line = reader.readLine();
			String[] tokens = line.split("\t");
			writer.write(tokens[0].toLowerCase() + ' ' + tokens[1].toLowerCase());
			while ((line = reader.readLine()) != null)
			{
				tokens = line.split("\t");
				writer.write(",\n" + tokens[0].toLowerCase() + ' ' + tokens[1].toLowerCase());
			}
			writer.write("\n)\n"
					+ "STORED AS ORC\n"
                    + "LOCATION '" + dataDir + "/" + tableName + "'\n"
					+ "TBLPROPERTIES (\"orc.compress\"=\"NONE\")");
		}
	}
	
	public static void GenCreateText (String schemaFilePath, String createStatementPath) throws IOException
	{
		try (BufferedReader reader = InputFactory.Instance().getReader(schemaFilePath);
			 BufferedWriter writer = OutputFactory.Instance().getWriter(createStatementPath))
		{
			String line;
			writer.write("create external table text\n(\n");
			line = reader.readLine();
			String[] tokens = line.split("\t");
			writer.write(tokens[0].toLowerCase() + ' ' + tokens[1].toLowerCase());
			while ((line = reader.readLine()) != null)
			{
				tokens = line.split("\t");
				writer.write(",\n" + tokens[0].toLowerCase() + ' ' + tokens[1].toLowerCase());
			}
			writer.write("\n)\n"
					+ "ROW FORMAT DELIMITED\n"
					+ "FIELDS TERMINATED BY '\\t'\n"
					+ "LOCATION '" + dataDir + "/" + ConfigFactory.Instance().getProperty("text.table.name") + "'");
		}
	}

	public static void GenCreateParq (String tableName, String schemaFilePath, String createStatementPath) throws IOException
	{
		try (BufferedReader reader = InputFactory.Instance().getReader(schemaFilePath);
			 BufferedWriter writer = OutputFactory.Instance().getWriter(createStatementPath);)
		{
			String line;
			writer.write("create external table " + tableName + "\n(\n");
			line = reader.readLine();
			String[] tokens = line.split("\t");
			writer.write(tokens[0].toLowerCase() + ' ' + tokens[1].toLowerCase());
			while ((line = reader.readLine()) != null)
			{
				tokens = line.split("\t");
				writer.write(",\n" + tokens[0].toLowerCase() + ' ' + tokens[1].toLowerCase());
			}
			writer.write("\n)\n"
					+ "STORED AS PARQUET\n"
                    + "LOCATION '" + dataDir + "/" + tableName + "'");
		}
	}
}
