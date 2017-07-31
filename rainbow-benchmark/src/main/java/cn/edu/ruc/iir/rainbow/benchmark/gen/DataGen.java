package cn.edu.ruc.iir.rainbow.benchmark.gen;

import cn.edu.ruc.iir.rainbow.benchmark.ColumnGenerator;
import cn.edu.ruc.iir.rainbow.benchmark.domain.ColumnArray;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark.gen
 * @ClassName: DataGen
 * @Description: To generate datas by ColumnArray
 * @author: Tao
 * @date: Create in 2017-07-30 15:22
 **/
public class DataGen {
    private int threadNum;

    private String data_origin = "_data/schema_origin.txt";
    private String filePath = null;
    private String columnName[];
    private List<ColumnArray> columnList = new ArrayList();

    public DataGen(int threadNum) {
        this.threadNum = threadNum;
    }

    /**
     * Generate _data by size
     *
     * @param dataSize size in MB
     */
    public void genDataBySize(int dataSize) {
        filePath = this.getClass().getClassLoader()
                .getResource((data_origin)).getFile();
        filePath = filePath.replace(data_origin, "");
        initColumns();
        initColumnRate();

        long genStartTime = System.currentTimeMillis();
        DataGenThread[] genThreads = new DataGenThread[threadNum];
        int size = Math.floorDiv(dataSize, threadNum);
        for (int i = 0; i < threadNum; i++) {
            DataGenThread t = new DataGenThread(filePath, columnName, columnList, size);
            genThreads[i] = t;
            t.run();
        }
        for (DataGenThread t : genThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long genEndTime = System.currentTimeMillis();
        System.out.println("gen runtime : ： " + (genEndTime - genStartTime) / 1000 + "s");
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
        ColumnGenerator columnGenerator = ColumnGenerator.Instance();
        columnName = columnGenerator.getColumnName();
    }
}
