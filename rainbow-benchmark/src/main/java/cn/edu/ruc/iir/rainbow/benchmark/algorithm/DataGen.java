package cn.edu.ruc.iir.rainbow.benchmark.algorithm;

import cn.edu.ruc.iir.rainbow.benchmark.DataGenerator;
import cn.edu.ruc.iir.rainbow.benchmark.domain.Column;
import cn.edu.ruc.iir.rainbow.benchmark.util.DataUtil;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark.algorithm
 * @ClassName: DataGen
 * @Description: To generate datas for benchmark with variable, refer to dict_data.txt & schema_new.txt
 * @author: Tao
 * @date: Create in 2017-07-29 15:54
 **/
public class DataGen {


    public int DATA_MAX = 40000;
    Random random = new Random();

    public String data_origin = "data/schema_origin.txt";
    public String filePath = null;
    String columnName[];
    List<List<Column>> columnList = new ArrayList();

    private static DataGen instance = null;

    public static DataGen Instance() {
        if (instance == null) {
            instance = new DataGen();
        }
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
        filePath = this.getClass().getClassLoader()
                .getResource((data_origin)).getFile();
        filePath = filePath.replace(data_origin, "");
        initColumns();
        initColumnRate();
        generateData(dataSize);
    }

    /**
     * @ClassName: DataGen
     * @Title:
     * @Description: To generate datas by dataSize, contains the algorithm(biSearch)
     * @param: dataSize -> *.GB
     * @date: 10:34 2017/7/30
     */
    private void generateData(int dataSize) {
        BufferedWriter bw = null;
        String directory = filePath + "Gen/" + DataUtil.getCurTime() + "/";
        String outGenPath = directory + DataUtil.getCurTime() + "_" + dataSize + ".txt";
        String outGenMemoPath = directory + "memo.txt";
        try {
            File f = new File(directory);
            if (!f.exists()) {
                // single directory
                f.mkdir();
            }
            bw = new BufferedWriter(new FileWriter(outGenPath));
            int col = 1;
            String fileSize = "";
            String writeLine = "";
            long fileS = 0L;
            while (true) {
                int randNum;
                String value = "";
                for (int i = 0; i < columnName.length; i++) {
                    List<Column> columnRate = columnList.get(i);
                    randNum = random.nextInt(DATA_MAX) + 1;
                    // binary search -> content
                    value = getValueByBinarySearch(randNum, columnRate);
                    writeLine += value;
                    if (i < columnName.length - 1)
                        writeLine += "\t";
                    col++;
                }
                writeLine += "\n";
                bw.write(writeLine);
                // at the end of each col, judge the size of the file
                fileS += writeLine.getBytes().length;
                DecimalFormat df = new DecimalFormat("#.00");
                if (fileS >= 1073741824) {
                    fileSize = df.format((double) fileS / 1073741824); // "G"
                    if (Double.valueOf(fileSize) >= dataSize) {
                        System.out.println("file is : " + fileSize);
                        break;
                    }
                } else if (fileS >= 1048576) {
                    fileSize = df.format((double) fileS / 1048576); // "M"
                    if (Double.valueOf(fileSize) >= 200) {
                        System.out.println("file is : " + fileSize);
                        break;
                    }
                }
            }
            // size, col -> memo.txt
            bw = new BufferedWriter(new FileWriter(outGenMemoPath));
            String memo = "{\"size\": \"" + fileSize + "\",\"colNum\":\"" + col + "\"}";
            bw.write(memo + "\n");
            System.out.println("memo is : " + memo);
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
     * @ClassName: DataGen
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
        return columnValue;
    }

    /**
     * @ClassName: DataGen
     * @Title:
     * @Description: To get the columns
     * @param:
     * @date: 19:02 2017/7/29
     */
    private void initColumns() {
        DataGenerator dataGenerator = DataGenerator.Instance();
        columnName = dataGenerator.getColumnName();
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
