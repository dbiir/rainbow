package cn.edu.ruc.iir.rainbow.layout.sql;

import cn.edu.ruc.iir.rainbow.common.util.InputFactory;
import cn.edu.ruc.iir.rainbow.common.util.OutputFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

public class GenDDL
{

	public static void CreateOrderedParq (String tableName) throws IOException
	{
		BufferedReader reader = InputFactory.Instance().getReader("cord-generator/resources/ordered_schema_1.txt");
		String line;
		BufferedWriter writer = OutputFactory.Instance().getWriter("cord-generator/resources/sql/create_ordered_parq.sql");
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
				+ "LOCATION '/" + tableName + "'");
		reader.close();
		writer.close();
	}

	public static void CreateOrderedOrc (String tableName) throws IOException
	{
		BufferedReader reader = InputFactory.Instance().getReader("cord-generator/resources/ordered_schema_1.txt");
		String line;
		BufferedWriter writer = OutputFactory.Instance().getWriter("cord-generator/resources/sql/create_ordered_orc.sql");
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
				+ "LOCATION '/" + tableName + "'\n"
		+ "TBLPROPERTIES (\"orc.compress\"=\"NONE\")");
		reader.close();
		writer.close();
	}
	
	public static void CreateText () throws IOException
	{
		BufferedReader reader = InputFactory.Instance().getReader("cord-generator/resources/schema.txt");
		String line;
		BufferedWriter writer = OutputFactory.Instance().getWriter("cord-generator/resources/sql/create_text.sql");
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
				+ "LOCATION '/text'");
		reader.close();
		writer.close();
	}

	public static void CreateParq (String tableName) throws IOException
	{
		BufferedReader reader = InputFactory.Instance().getReader("cord-generator/resources/schema.txt");
		String line;
		BufferedWriter writer = OutputFactory.Instance().getWriter("cord-generator/resources/sql/create_parq.sql");
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
				+ "LOCATION '/" + tableName + "'");
		reader.close();
		writer.close();
	}
	
	public static void LoadParq(boolean overWrite, String tableName) throws IOException
	{
		BufferedReader reader = InputFactory.Instance().getReader("cord-generator/resources/ordered_schema_1.txt");
		String line;
		BufferedWriter writer = OutputFactory.Instance().getWriter("cord-generator/resources/sql/load_ordered_parq.sql");
		writer.write("INSERT " + (overWrite ? "OVERWRITE" : "INTO") + " TABLE " + tableName + "\nSELECT\n");
		line = reader.readLine();
		writer.write(line.split("\t")[0].toLowerCase());
		while ((line = reader.readLine()) != null)
		{
			writer.write(",\n" + line.split("\t")[0].toLowerCase());
		}
		writer.write("\nfrom text\n");
		//writer.write("INSERT OVERWRITE TABLE parq SELECT * from text;\n");
		reader.close();
		writer.close();
	}

	public static void LoadOrc(boolean overWrite, String tableName) throws IOException
	{
		BufferedReader reader = InputFactory.Instance().getReader("cord-generator/resources/ordered_schema_1.txt");
		String line;
		BufferedWriter writer = OutputFactory.Instance().getWriter("cord-generator/resources/sql/load_ordered_orc.sql");
		writer.write("INSERT " + (overWrite ? "OVERWRITE" : "INTO") + " TABLE " + tableName + "\nSELECT\n");
		line = reader.readLine();
		writer.write(line.split("\t")[0].toLowerCase());
		while ((line = reader.readLine()) != null)
		{
			writer.write(",\n" + line.split("\t")[0].toLowerCase());
		}
		writer.write("\nfrom text\n");
		//writer.write("INSERT OVERWRITE TABLE parq SELECT * from text;\n");
		reader.close();
		writer.close();
	}
	
	public static void main(String[] args) throws IOException
	{
		//CreateOrderedParq ("ordered_parq_43");
		//CreateText();
		//CreateParq("parq_43");
		//LoadParq(false, "ordered_parq_43");

		CreateOrderedOrc ("ordered_orc");
		//CreateText();
		//CreateParq("parq_43");
		LoadOrc(false, "ordered_orc");
	}

}
