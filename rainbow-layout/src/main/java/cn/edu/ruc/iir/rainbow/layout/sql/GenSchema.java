package cn.edu.ruc.iir.rainbow.layout.sql;

import cn.edu.ruc.iir.rainbow.common.util.InputFactory;
import cn.edu.ruc.iir.rainbow.common.util.OutputFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

/**
 * Created by hank on 16-12-30.
 */
public class GenSchema
{
    public static void main(String[] args) throws IOException
    {
        BufferedWriter writer = OutputFactory.Instance().getWriter("cord-generator/resources/schema.txt");
        BufferedReader originSchemaReader = InputFactory.Instance().getReader("cord-generator/resources/schema_origin.txt");
        BufferedReader columnSizeReader = InputFactory.Instance().getReader("cord-generator/resources/column_size.txt");
        String lineNameType = null, lineNameSize = null;
        while ((lineNameType=originSchemaReader.readLine()) != null)
        {
            String[] nameType = lineNameType.split("\t");
            lineNameSize = columnSizeReader.readLine();
            String[] nameSize = lineNameSize.split("\t");
            if (nameSize[0].equalsIgnoreCase(nameType[0]))
            {
                writer.write(nameType[0] + "\t" + nameType[1] + "\t" + nameSize[1] + "\n");
            }
            else
            {
                System.out.println(nameSize[0] + ", " + nameType[0]);
            }
        }
        originSchemaReader.close();
        columnSizeReader.close();
        writer.close();
    }
}
