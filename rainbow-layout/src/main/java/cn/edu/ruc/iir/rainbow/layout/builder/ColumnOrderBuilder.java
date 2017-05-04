package cn.edu.ruc.iir.rainbow.layout.builder;

import cn.edu.ruc.iir.rainbow.layout.domian.Column;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hank on 2015/4/28.
 */
public class ColumnOrderBuilder
{
    private ColumnOrderBuilder () {}

    public static List<Column> build (File columnOrderFile) throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader(columnOrderFile));

        List<Column> columnOrder = new ArrayList<Column>();
        String line;
        int cid = 0;
        while ((line = reader.readLine()) != null)
        {
            String[] tokens = line.split("\t");
            Column column = new Column(cid, tokens[0], tokens[1], Double.parseDouble(tokens[2]));
            columnOrder.add(column);
            ++cid;
        }

        reader.close();

        return columnOrder;
    }

    public static void saveToFile (File columnOrderFile, List<Column> columnOrder) throws IOException
    {
        BufferedWriter writer = new BufferedWriter(new FileWriter(columnOrderFile));

        for (Column column : columnOrder)
        {
            writer.write(column.getName() + "\t" + column.getType() + "\t" + column.getSize() + "\n");
        }

        writer.close();
    }

    public static void saveAsDDLSegment (File columnOrderFile, List<Column> columnOrder) throws IOException
    {
        BufferedWriter writer = new BufferedWriter(new FileWriter(columnOrderFile));

        for (Column column : columnOrder)
        {
            String columnName = column.getName();
            if (column.isDuplicated())
            {
                columnName += "_" + column.getDupId();
            }
            writer.write(columnName + "\t" + column.getType() + "\t" + column.getSize() + "\n");
        }

        writer.close();
    }
}
