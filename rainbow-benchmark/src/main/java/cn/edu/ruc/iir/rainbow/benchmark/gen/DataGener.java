package cn.edu.ruc.iir.rainbow.benchmark.gen;

import cn.edu.ruc.iir.rainbow.benchmark.DataGenerator;
import cn.edu.ruc.iir.rainbow.benchmark.domain.ColumnArray;
import cn.edu.ruc.iir.rainbow.benchmark.util.DataUtil;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark.gen
 * @ClassName: DataGener
 * @Description: To generate datas by ColumnArray
 * @author: Tao
 * @date: Create in 2017-07-30 15:22
 **/
public class DataGener {

    public Long fileS = 0L;

    public int DATA_MAX = 40000;
    Random random = new Random();

    public String data_origin = "data/schema_origin.txt";
    public String filePath = null;
    String columnName[];
    List<ColumnArray> columnList = new ArrayList();

    private static DataGener instance = new DataGener();

    private DataGener() {
    }

    public static DataGener getInstance() {
        return instance;
    }

    public void genDataBySize(int dataSize) {
        long startTime = System.currentTimeMillis();
        filePath = this.getClass().getClassLoader()
                .getResource((data_origin)).getFile();
        filePath = filePath.replace(data_origin, "");
        initColumns();
        initColumnRate();

        long endTime = System.currentTimeMillis();
        System.out.println("init runtime : ： " + (endTime - startTime) / 1000 + "s");
        generateData(1);
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
            int col = 1;
            String fileSize = "";
            while (true) {
                StringBuilder writeLine = new StringBuilder();
                int randNum;
                String value = "";

//                long startTime = System.currentTimeMillis();
                for (int i = 0; i < columnName.length; i++) {
                    ColumnArray colArray = columnList.get(i);
                    randNum = random.nextInt(DATA_MAX);
                    // binary search -> content
                    value = colArray.getArray()[randNum];
                    writeLine.append(value);
                    if (i < columnName.length - 1)
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
                if (fileS >= 1073741824) {
                    fileSize = df.format((double) fileS / 1073741824); // "G"
                    if (Double.valueOf(fileSize) >= dataSize) {
                        break;
                    }
                } else if (fileS >= 1048576) {
                    fileSize = df.format((double) fileS / 1048576); // "M"
                    if (Double.valueOf(fileSize) >= 200) {
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


    private void initColumnRate() {
        for (String cName : columnName) {
            initColumnList(cName);
        }
    }

    private void initColumnList(String cName) {
        String columnPath = filePath + "data_dict/" + cName + ".txt";
        String curLine = null;
        BufferedReader br = null;
        String columnsLine[] = null;
        try {
            br = new BufferedReader(new FileReader(columnPath));
            int top = 0, bottom = 0;
            ColumnArray c = new ColumnArray();
            while ((curLine = br.readLine()) != null) {
                // 0: columnName, 1: content, 2: rate(interval)
                columnsLine = curLine.split("\t");
                // 0: columnName, 1: content, 2: rate(interval)
                top += Integer.valueOf(columnsLine[2]);
                for (int i = bottom; i < top; i++)
                    c.getArray()[i] = columnsLine[1];
                bottom = top;
            }
            columnList.add(c);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null)
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    private void initColumns() {
        DataGenerator dataGenerator = DataGenerator.Instance();
        columnName = dataGenerator.getColumnName();
    }


}
