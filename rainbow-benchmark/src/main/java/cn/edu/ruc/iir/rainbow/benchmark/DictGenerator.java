package cn.edu.ruc.iir.rainbow.benchmark;

import cn.edu.ruc.iir.rainbow.benchmark.util.DataUtil;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark
 * @ClassName: DictGenerator
 * @Description: get data dicts with the statistic
 * @author: Tao
 * @date: Create in 2017-07-28 7:08
 **/
public class DictGenerator {

    //    public String data_test = "data/test.txt";
    public String data_test = "data/40000-line.txt";
    public String filePath = null;

    private static DictGenerator instance = null;

    public static DictGenerator Instance() {
        if (instance == null) {
            instance = new DictGenerator();
        }
        return instance;
    }

    /**
     * @ClassName: DataGenerator
     * @Title:
     * @Description: get data dicts with the statistic
     * @param:
     * @problem: cost time much
     * @date: 16:55 2017/7/27
     */
    public void getDataDict() {
        String curLine = null;
        BufferedReader br = null;
        BufferedWriter bw = null;
        DataGenerator dataGenerator = DataGenerator.Instance();
        String columnName[] = dataGenerator.getColumnName();
        filePath = this.getClass().getClassLoader()
                .getResource((data_test)).getFile();
        String outDictPath = filePath.replace(data_test, "data_dict.txt");
        String columnsLine[] = null;
        String columnDict[][] = new String[columnName.length][1];
        try {
            br = new BufferedReader(new FileReader(filePath));
            bw = new BufferedWriter(new FileWriter(outDictPath));
            int i = 0;
            while ((curLine = br.readLine()) != null) {
                columnsLine = curLine.split("\t");
                if (i == 0) {
                    for (int k = 0; k < columnsLine.length; k++) {
                        columnDict[k][0] = columnsLine[k] + ",";
                    }
                } else {
                    for (int k = 0; k < columnsLine.length; k++) {
                        if (!DataUtil.isContainsStr(columnDict[k][0], columnsLine[k]))
                            columnDict[k][0] += columnsLine[k] + ",";
                    }
                }
                i++;
                if (i % 100 == 0) {
                    System.out.println(i);
                }
            }
            for (int j = 0; j < columnName.length; j++) {
                bw.write(columnName[j] + "\n");
                bw.write(columnDict[j][0].substring(0, columnDict[j][0].length() - 1) + "\n\n");
            }
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

    /**
     * @ClassName: DictGenerator
     * @Title:
     * @Description: By map
     * @param: colunmIndex -> the index of the columns
     * @date: 21:57 2017/7/28
     */
    public void getDataDict(int columnIndex) {
        String curLine = null;
        BufferedReader br = null;
        BufferedWriter bw = null;
        // note that "filePath" is contained in DataGenerator.java too
        filePath = this.getClass().getClassLoader()
                .getResource((data_test)).getFile();
        DataGenerator dataGenerator = DataGenerator.Instance();
        String columnName[] = dataGenerator.getColumnName();
        String outDictPath = null;
        String columnsLine[] = null;
        String tempStr = null;
        Map<String, Integer> map = new HashMap<>();
        try {
            br = new BufferedReader(new FileReader(filePath));
            int i = 0;
            while ((curLine = br.readLine()) != null) {
                columnsLine = curLine.split("\t");
                tempStr = columnsLine[columnIndex];
                int num = 1;
                if (map.get(tempStr) != null) {
                    num = map.get(tempStr);
                    num++;
                } else {
                    num = 1;
                }
                map.put(tempStr, num);
                i++;
                if (i % 10000 == 0) {
                    System.out.println(columnName[columnIndex] + " is : " + i);
                }
            }
            outDictPath = filePath.replace(data_test, "data_dict/" + columnName[columnIndex] + ".txt");
            // to make column.txt with every column
            bw = new BufferedWriter(new FileWriter(outDictPath));
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
//                System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                bw.write(columnName[columnIndex] + "\t" + entry.getKey() + "\t" + entry.getValue() + "\n");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null || bw != null)
                try {
                    br.close();
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    /**
     * @ClassName: DictGenerator
     * @Title:
     * @Description: By map, make a columnName.txt for each column
     * @param: colunmIndex -> the index of the columns, columnName[] -> from the schema.txt
     * @date: 0:01 2017/7/29
     */
    public void getDataDict(int columnIndex, String columnName[]) {
        String curLine = null;
        BufferedReader br = null;
        BufferedWriter bw = null;
        // note that "filePath" is contained in DataGenerator.java too
        filePath = this.getClass().getClassLoader()
                .getResource((data_test)).getFile();
        String outDictPath = null;
        String columnsLine[] = null;
        String tempStr = null;
        Map<String, Integer> map = new HashMap<>();
        try {
            br = new BufferedReader(new FileReader(filePath));
            int i = 0;
            while ((curLine = br.readLine()) != null) {
                columnsLine = curLine.split("\t");
                tempStr = columnsLine[columnIndex];
                int num = 1;
                if (map.get(tempStr) != null) {
                    num = map.get(tempStr);
                    num++;
                } else {
                    num = 1;
                }
                map.put(tempStr, num);
                i++;
                if (i % 10000 == 0) {
                    System.out.println(columnName[columnIndex] + " is : " + i);
                }
            }
            outDictPath = filePath.replace(data_test, "data_dict/" + columnName[columnIndex] + ".txt");
            // to make column.txt with every column
            bw = new BufferedWriter(new FileWriter(outDictPath));
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                bw.write(columnName[columnIndex] + "\t" + entry.getKey() + "\t" + entry.getValue() + "\n");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null || bw != null)
                try {
                    br.close();
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

}
