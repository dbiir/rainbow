package cn.edu.ruc.iir.rainbow.benchmark.generator;

import cn.edu.ruc.iir.rainbow.benchmark.util.SysSettings;
import cn.edu.ruc.iir.rainbow.benchmark.domain.Column;
import cn.edu.ruc.iir.rainbow.benchmark.util.DataUtil;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark.generator
 * @ClassName: DataGenerator
 * @Description: To generate datas for benchmark with variable, refer to dict_data.txt & schema_new.txt
 * @author: Tao
 * @date: Create in 2017-07-29 15:54
 **/
public class DataGenerator
{

    private String filePath;
    private String columnName[];
    private List<List<Column>> columnList = new ArrayList();
    private int threadNum;

    public static final String SCHEMA_FILE = "schema.txt";

    private static DataGenerator instance = new DataGenerator();

    private DataGenerator()
    {
    }

    public static DataGenerator getInstance(int threadNum)
    {
        instance.threadNum = threadNum;
        return instance;
    }

    /**
     * @ClassName: DataGenerator
     * @Title:
     * @Description: To generate data by dataSize
     * @param: dataSize -> n MB
     * @date: 16:15 2017/7/29
     */
    public void genDataBySize(int dataSize)
    {
        initColumns();
        initColumnRate();

        long startTime = System.currentTimeMillis();
        DataGeneratorThread[] dataGeneratorThreads = new DataGeneratorThread[threadNum];
        int size = Math.floorDiv(dataSize, threadNum);
        try
        {
            filePath = SysSettings.TEMPLATE_DIRECTORY + "/rainbow_" + DataUtil.getCurTime() + "_" + dataSize + "MB/";
            for (int i = 0; i < threadNum; i++)
            {
                DataGeneratorThread t = new DataGeneratorThread(filePath, columnName, columnList, size);
                dataGeneratorThreads[i] = t;
                t.run();
            }
            for (DataGeneratorThread t : dataGeneratorThreads)
            {
                try
                {
                    t.join();
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
    }

    /**
     * @ClassName: DataGenerator
     * @Title:
     * @Description: To get the columns
     * @param:
     * @date: 19:02 2017/7/29
     */
    private void initColumns()
    {
        String filePath = SysSettings.TEMPLATE_DIRECTORY + "/" + SCHEMA_FILE;
        String curLine;
        BufferedReader br = null;
        String splitLine[];
        StringBuilder columnLine = new StringBuilder();
        try
        {
            br = new BufferedReader(new FileReader(filePath));
            while ((curLine = br.readLine()) != null)
            {
                splitLine = curLine.split("\t");
                columnLine.append(splitLine[0] + ",");
            }
            columnLine.deleteCharAt(columnLine.length() - 1);
            this.columnName = columnLine.toString().split(",");
        } catch (FileNotFoundException e)
        {
            System.out.println("error: " + filePath);
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        } finally
        {
            if (br != null)
                try
                {
                    br.close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
        }
    }

    /**
     * @ClassName: DataGenerator
     * @Title:
     * @Description: To get the rate of column in each *.txt
     * @param:
     * @date: 17:30 2017/7/29
     */
    private void initColumnRate()
    {
        for (String cName : columnName)
        {
            initColumnList(cName);
        }
    }

    /**
     * @ClassName: DataGenerator
     * @Title:
     * @Description: To fill columnMap with *.txt
     * @param:
     * @date: 19:18 2017/7/29
     */
    private void initColumnList(String cName)
    {
        String columnPath = SysSettings.TEMPLATE_DIRECTORY + "/column_templates/" + cName + ".txt";
        String curLine = null;
        BufferedReader br = null;
        String columnsLine[] = null;
        try
        {
            br = new BufferedReader(new FileReader(columnPath));
            List<Column> columnRate = new ArrayList<>();
            int index = 0;
            while ((curLine = br.readLine()) != null)
            {
                // 0: columnName, 1: content, 2: rate(interval)
                columnsLine = curLine.split("\t");
                // 0: columnName, 1: content, 2: rate(interval)
                index += Integer.valueOf(columnsLine[2]);
                Column c = new Column(index, columnsLine[1]);
                columnRate.add(c);
            }
            columnList.add(columnRate);
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        } finally
        {
            if (br != null)
                try
                {
                    br.close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
        }
    }

}
