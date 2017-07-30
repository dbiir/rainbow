package cn.edu.ruc.iir.rainbow.benchmark.gen;

import cn.edu.ruc.iir.rainbow.benchmark.domain.ColumnArray;
import cn.edu.ruc.iir.rainbow.benchmark.util.DataUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Random;

/**
 * rainbow
 *
 * @author guodong
 */
public class DataGenThread
        extends Thread {
    private String filePath;
    private String[] columnNames;
    private List<ColumnArray> columnList;
    private int size;

    private Random random;
    private long fileS = 0L;

    private final int DATA_MAX = 40000;
    private final long MB1 = 1 * 1024 * 1024L;

    public DataGenThread(String filePath, String[] columnNames, List<ColumnArray> columnList, int size) {
        this.filePath = filePath;
        this.columnNames = columnNames;
        this.columnList = columnList;
        this.size = size;
        this.random = new Random();
    }

    @Override
    public void run() {
        generateData(size);
    }

    private void generateData(int dataSize) {
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
            int col = 0;
            String fileSize = "";
            while (true) {
                StringBuilder writeLine = new StringBuilder();
                int randNum;
                String value = "";
//                long startTime = System.currentTimeMillis();
                for (int i = 0; i < columnNames.length; i++) {
                    ColumnArray colArray = columnList.get(i);
                    randNum = random.nextInt(DATA_MAX);
                    // binary search -> content
                    value = colArray.getArray()[randNum];
                    writeLine.append(value);
                    if (i < columnNames.length - 1)
                        writeLine.append("\t");
                }
//                long endTime = System.currentTimeMillis();
//                System.out.println("getArray & set runtime : ： " + (endTime - startTime) + "ms");

                writeLine.append("\n");
                bw.write(writeLine.toString());
                col++;
                // at the end of each col, judge the size of the file
                fileS += writeLine.toString().getBytes().length;
                // set "", start next line to write
                DecimalFormat df = new DecimalFormat("#.00");
                fileSize = df.format((double) fileS / MB1);
                if (Double.valueOf(fileSize) >= dataSize) {
                    break;
                }
            }
            bw.flush();
            // size, col -> memo.txt
            bw = new BufferedWriter(new FileWriter(outGenMemoPath));
            String memo = "{\"size\": \"" + fileSize + "\",\"colNum\":\"" + col + "\"}";
            bw.write(memo + "\n");
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
}
