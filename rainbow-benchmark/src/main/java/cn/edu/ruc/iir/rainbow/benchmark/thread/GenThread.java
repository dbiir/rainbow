package cn.edu.ruc.iir.rainbow.benchmark.thread;

import cn.edu.ruc.iir.rainbow.benchmark.domain.Column;
import cn.edu.ruc.iir.rainbow.benchmark.util.DataUtil;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark.thread
 * @ClassName: GenThread
 * @Description: To make threads to run the programe
 * @author: Tao
 * @date: Create in 2017-07-30 15:59
 **/
public class GenThread extends Thread {

    public Long fileS = 0L;

    public int DATA_MAX = 40000;
    Random random = new Random();

    public String filePath = null;
    String columnName[];
    List<List<Column>> columnList = new ArrayList();

    public GenThread() {
    }

    public GenThread(String filePath, String[] columnName, List<List<Column>> columnList) {
        this.filePath = filePath;
        this.columnName = columnName;
        this.columnList = columnList;
    }

    @Override
    public void run() {
        try {
            // programe runtime
            generateData(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @ClassName: GenThread
     * @Title:
     * @Description: To generate datas by dataSize, contains the algorithm(biSearch)
     * @param: dataSize -> *.GB
     * @date: 10:34 2017/7/30
     */
    public void generateData(int dataSize) {
        BufferedWriter bw = null;
        String directory = filePath + "GenThread/" + DataUtil.getCurTime() + "/";
        String outGenPath = directory + DataUtil.getCurTime() + "_" + dataSize + ".txt";
        String outGenMemoPath = directory + "memo.txt";
        try {
            File f = new File(directory);
            if (!f.exists()) {
                f.mkdirs();
            }
            bw = new BufferedWriter(new FileWriter(outGenPath), 1024 * 1024 * 32);
            int col = 1;
            String fileSize = "";
            while (true) {
                StringBuilder writeLine = new StringBuilder();
                int randNum;
                String value = "";
                for (int i = 0; i < columnName.length; i++) {
                    List<Column> columnRate = columnList.get(i);
                    randNum = random.nextInt(DATA_MAX) + 1;
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
                if (fileS >= 1073741824) {
                    fileSize = df.format((double) fileS / 1073741824); // "G"
                    if (Double.valueOf(fileSize) >= dataSize) {
                        break;
                    }
                } else if (fileS >= 1048576) {
                    fileSize = df.format((double) fileS / 1048576); // "M"
                    if (Double.valueOf(fileSize) >= 100) {
                        break;
                    }
                }
            }
            bw.flush();
            // size, col -> memo.txt
            bw = new BufferedWriter(new FileWriter(outGenMemoPath));
            String memo = "{\"size\": \"" + fileSize + "\",\"colNum\":\"" + col + "\"}";
            bw.write(memo + "\n");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bw != null)
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    /**
     * @ClassName: GenThread
     * @Title:
     * @Description: To search the interval of the random number
     * @param: randNum: the input random number, columnRate: the list of domain(Column)
     * @date: 8:37 2017/7/30
     */
    protected String getValueByBinarySearch(int randNum, List<Column> columnRate) {
        String columnValue = null;
        int lo = 0;
        int hi = columnRate.size() - 1;
        int mid;
        int top;
        Column col, col1;
        while (lo <= hi) {
            mid = (lo + hi) / 2;
            col = columnRate.get(mid);
            top = col.getUpperBound();
            if (mid > 0) {
                col1 = columnRate.get(mid - 1);
                if (top >= randNum && randNum > col1.getUpperBound()) {
                    columnValue = columnRate.get(mid).getValue();
                    return columnValue;
                } else if (top < randNum) {
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            } else {
                if (top >= randNum && randNum > 0) {
                    columnValue = columnRate.get(mid).getValue();
                    return columnValue;
                } else if (top < randNum) {
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }
        }
        columnValue = "search_error";
        System.out.println("Problem is : " + columnValue);
        return columnValue;
    }

}
