package cn.edu.ruc.iir.rainbow.layout.sql;

import cn.edu.ruc.iir.rainbow.common.util.OutputFactory;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * Created by hank on 2015/3/22.
 */
public class GenGroupedDDL
{
    public static void CreateGroupedText (String tableName, int groupSize) throws IOException
    {
        BufferedWriter writer = OutputFactory.Instance().getWriter("cord-generator/resources/sql/create_grouped_text.sql");
        writer.write("create external table " + tableName + "\n(\n");
        writer.write("s0 array<string>");
        for (int i = 1; i <= (1187/groupSize); ++i)
        {
            writer.write(",\ns" + i + " array<string>");
        }
        writer.write("\n)\n"
                + "ROW FORMAT DELIMITED\n"
                + "FIELDS TERMINATED BY '\\t'\n"
                + "COLLECTION ITEMS TERMINATED BY ':'\n"
                + "LOCATION '/ordered_grouped_text'");
        writer.close();
    }

    public static void CreateGroupedParq (String tableName, int groupSize) throws IOException
    {
        BufferedWriter writer = OutputFactory.Instance().getWriter("cord-generator/resources/sql/create_grouped_parq.sql");
        writer.write("create external table " + tableName + "\n(\n");
        writer.write("s0 array<string>");
        for (int i = 1; i <= (1187/groupSize); ++i)
        {
            writer.write(",\ns" + i + " array<string>");
        }
        writer.write("\n)\n"
                + "STORED AS PARQUET\n"
                + "LOCATION '/ordered_grouped_parq'");
        writer.close();
    }


    public static void main(String[] args) throws IOException
    {
        CreateGroupedText("grouped_text", 10);
        CreateGroupedParq("grouped_parq", 10);
    }
}
