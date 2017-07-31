package cn.edu.ruc.iir.rainbow.benchmark.algorithm;

import cn.edu.ruc.iir.rainbow.benchmark.ColumnGenerator;
import cn.edu.ruc.iir.rainbow.benchmark.domain.Column;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark.algorithm
 * @ClassName: DataGen
 * @Description: To generate datas for benchmark with variable, refer to dict_data.txt & schema_new.txt
 * @author: Tao
 * @date: Create in 2017-07-29 15:54
 **/
public class DataGenerator {

    private String data_origin = "_data/schema_origin.txt";
    private String filePath = "";
    private String columnName[];
    private List<List<Column>> columnList = new ArrayList();
    private int threadNum;

    private static DataGenerator instance = new DataGenerator();

    private DataGenerator() {
    }

    public static DataGenerator getInstance(int threadNum) {
        instance.threadNum = threadNum;
        return instance;
    }


    /**
     * @ClassName: DataGen
     * @Title:
     * @Description: To generate datas by dataSize
     * @param: dataSize -> n GB (200MB, 4W row)
     * @date: 16:15 2017/7/29
     */
    public void genDataBySize(int dataSize) {
        // init, 1718ms
        filePath = this.getClass().getClassLoader()
                .getResource((data_origin)).getFile();
        filePath = filePath.replace(data_origin, "");
        initColumns();
        initColumnRate();

        long startTime = System.currentTimeMillis();
        DataGeneratorThread[] dataGeneratorThreads = new DataGeneratorThread[threadNum];
        int size = Math.floorDiv(dataSize, threadNum);
        try {
            for (int i = 0; i < threadNum; i++) {
                DataGeneratorThread t = new DataGeneratorThread(filePath, columnName, columnList, size);
                dataGeneratorThreads[i] = t;
                t.run();
            }
            for (DataGeneratorThread t : dataGeneratorThreads) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("gen thread run time : ： " + (endTime - startTime) / 1000 + "s");
    }

    /**
     * @ClassName: DataGen
     * @Title:
     * @Description: To get the columns
     * @param:
     * @date: 19:02 2017/7/29
     */
    private void initColumns() {
        ColumnGenerator columnGenerator = ColumnGenerator.Instance();
        columnName = columnGenerator.getColumnName();
    }

    /**
     * @ClassName: DataGen
     * @Title:
     * @Description: To get the rate of column in each *.txt
     * @param:
     * @date: 17:30 2017/7/29
     */
    private void initColumnRate() {
        for (String cName : columnName) {
            initColumnList(cName);
        }
    }

    /**
     * @ClassName: DataGen
     * @Title:
     * @Description: To fill columnMap with *.txt
     * @param:
     * @date: 19:18 2017/7/29
     */
    private void initColumnList(String cName) {
        String columnPath = filePath + "data_dict/" + cName + ".txt";
        String curLine = null;
        BufferedReader br = null;
        String columnsLine[] = null;
        try {
            br = new BufferedReader(new FileReader(columnPath));
            List<Column> columnRate = new ArrayList<>();
            int index = 0;
            while ((curLine = br.readLine()) != null) {
                Column c = new Column();
                // 0: columnName, 1: content, 2: rate(interval)
                columnsLine = curLine.split("\t");
                // 0: columnName, 1: content, 2: rate(interval)
                index += Integer.valueOf(columnsLine[2]);
                c.setUpperBound(index);
                c.setValue(columnsLine[1]);
                columnRate.add(c);
            }
            columnList.add(columnRate);
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

}
