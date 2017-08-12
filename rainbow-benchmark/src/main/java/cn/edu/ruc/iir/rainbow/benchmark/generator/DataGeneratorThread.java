package cn.edu.ruc.iir.rainbow.benchmark.generator;

import cn.edu.ruc.iir.rainbow.benchmark.util.SysSettings;
import cn.edu.ruc.iir.rainbow.benchmark.domain.Column;
import cn.edu.ruc.iir.rainbow.benchmark.util.DataUtil;

import java.io.*;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Random;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark.generator
 * @ClassName: DataGeneratorThread
 * @Description: To make threads to run the programe
 * @author: Tao
 * @date: Create in 2017-07-30 15:59
 **/
public class DataGeneratorThread extends Thread
{

    // parameters needed to be created
    private String filePath;
    private String columnName[];
    private List<List<Column>> columnList;
    private int dataSize;
    private Random random;

    private Long fileS = 0L;

    public DataGeneratorThread()
    {
    }

    public DataGeneratorThread(String filePath, String[] columnName, List<List<Column>> columnList, int dataSize)
    {
        this.filePath = filePath;
        this.columnName = columnName;
        this.columnList = columnList;
        this.dataSize = dataSize;
        this.random = new Random();
    }

    @Override
    public void run()
    {
        try
        {
            // program runtime
            generateData(dataSize);
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * @ClassName: DataGeneratorThread
     * @Title:
     * @Description: To generate datas by dataSize, contains the generator(biSearch)
     * @param: dataSize -> *.MB
     * @date: 10:34 2017/7/30
     */
    public void generateData(int dataSize)
    {
        BufferedWriter bw = null;
        String fileName = DataUtil.getCurTime();
        String directory = filePath + "data/";
        String outGenPath = directory + fileName + ".txt";
        String outGenMemoPath = filePath + "memo.txt";
        try
        {
            File f = new File(directory);
            if (!f.exists())
            {
                f.mkdirs();
            }
            bw = new BufferedWriter(new FileWriter(outGenPath), SysSettings.BUFFER_SIZE);
            int col = 0;
            String fileSize;
            int randNum;
            while (true)
            {
                StringBuilder writeLine = new StringBuilder();
                String value;
                for (int i = 0; i < columnName.length; i++)
                {
                    List<Column> columnRate = columnList.get(i);
                    randNum = random.nextInt(SysSettings.DATA_MAX) + 1;
                    // binary search -> content
                    value = getValueByBinarySearch(randNum, columnRate);
                    writeLine.append(value);
                    if (i < columnName.length - 1)
                        writeLine.append("\t");
                }
                col++;
                writeLine.append("\n");
                bw.write(writeLine.toString());
                // at the end of each col, judge the size of the file
                fileS += writeLine.toString().getBytes().length;
                // set "", start next line to write
                DecimalFormat df = new DecimalFormat("#.00");
                fileSize = df.format((double) fileS / SysSettings.MB);
                if (Double.valueOf(fileSize) >= dataSize)
                {
                    break;
                }
            }
            bw.flush();
            bw.close();
            // size, col -> memo.txt
            bw = new BufferedWriter(new FileWriter(outGenMemoPath, true));
            String memo = "file name: " + fileName + ", number of columns: " + col;
            bw.write(memo + "\n");
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        } finally
        {
            if (bw != null)
                try
                {
                    bw.close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
        }
    }

    /**
     * @ClassName: DataGeneratorThread
     * @Title:
     * @Description: To search the interval of the random number
     * @param: randNum: the input random number, columnRate: the list of domain(Column)
     * @date: 8:37 2017/7/30
     */
    protected String getValueByBinarySearch(int randNum, List<Column> columnRate)
    {
        String columnValue;
        int lo = 0;
        int hi = columnRate.size() - 1;
        int mid;
        int top;
        Column col, col1;
        while (lo <= hi)
        {
            mid = (lo + hi) / 2;
            col = columnRate.get(mid);
            top = col.getUpperBound();
            if (mid > 0)
            {
                col1 = columnRate.get(mid - 1);
                if (top >= randNum && randNum > col1.getUpperBound())
                {
                    columnValue = columnRate.get(mid).getValue();
                    return columnValue;
                } else if (top < randNum)
                {
                    lo = mid + 1;
                } else
                {
                    hi = mid - 1;
                }
            } else
            {
                if (top >= randNum && randNum > 0)
                {
                    columnValue = columnRate.get(mid).getValue();
                    return columnValue;
                } else if (top < randNum)
                {
                    lo = mid + 1;
                } else
                {
                    hi = mid - 1;
                }
            }
        }
        columnValue = "search_error";
        System.out.println("Problem is : " + columnValue);
        return columnValue;
    }

}
