package cn.edu.ruc.iir.rainbow.benchmark;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark
 * @ClassName: DataGenerator
 * @Description: To generate data for test
 * @author: Tao
 * @date: Create in 2017-07-27 14:20
 **/
public class DataGenerator {

    public String schema_origin = "data/schema.txt";
    public String workload = "data/workload.txt";
    public String filePath = null;

    // column mapping
    public Map<String, String> columnMap = new HashMap<String, String>();

    private static DataGenerator instance = null;

    public static DataGenerator Instance() {
        if (instance == null) {
            instance = new DataGenerator();
        }
        return instance;
    }

    /**
     * @ClassName: DataGenerator
     * @Title:
     * @Description: Change Column, make column_mapping.csv & schema_new.txt
     * @param:
     * @date: 16:24 2017/7/27
     */
    public void setColumnShift() {
        filePath = this.getClass().getClassLoader()
                .getResource((schema_origin)).getFile();
        String curLine = null;
        BufferedReader br = null;
        BufferedWriter bw = null;
        BufferedWriter bw1 = null;
        String outCsvPath = filePath.replace(schema_origin, "column_mapping.csv");
        String outSchemaPath = filePath.replace(schema_origin, "schema_new.txt");
        int i = 1;
        String newColumnName = null;
        String mapLine[] = null;
        try {
            br = new BufferedReader(new FileReader(filePath));
            bw = new BufferedWriter(new FileWriter(outCsvPath));
            bw1 = new BufferedWriter(new FileWriter(outSchemaPath));
            while ((curLine = br.readLine()) != null) {
                mapLine = curLine.split("\t");
                newColumnName = "Column_" + i++;
                bw.write(mapLine[0] + "," + newColumnName + "\n");
                bw1.write(newColumnName + "\t" + mapLine[1] + "\t" + mapLine[2] + "\n");
                columnMap.put(mapLine[0], newColumnName);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null || bw != null || bw1 != null)
                try {
                    br.close();
                    bw.close();
                    bw1.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    /**
     * @ClassName: DataGenerator
     * @Title:
     * @Description: make workload_new.csv
     * @param:
     * @date: 16:28 2017/7/27
     */
    public void setWorkloadShift() {
        filePath = this.getClass().getClassLoader()
                .getResource((workload)).getFile();
        String curLine = null;
        BufferedReader br = null;
        BufferedWriter bw = null;
        String outWorkloadPath = filePath.replace(workload, "workload_new.txt");
        String splitLine[] = null;
        String columnsLine[] = null;
        String columnName = null;
        try {
            br = new BufferedReader(new FileReader(filePath));
            bw = new BufferedWriter(new FileWriter(outWorkloadPath));
            while ((curLine = br.readLine()) != null) {
                splitLine = curLine.split("\t");
                // set column1 only
                // curLine = curLine.replace(splitLine[0], UUID.randomUUID().toString());
                columnsLine = splitLine[2].split(",");
                for (int k = 0; k < columnsLine.length; k++) {
                    columnName = columnMap.get(columnsLine[k]);
                    if (columnName != null) {
                        // According to the column, each once, not replace all -> "abc, abcd"(note)
                        // or generate a new String(the same to replace)
                        curLine = curLine.replaceFirst(columnsLine[k], columnName);
                    } else {
                        System.out.println(columnsLine[k] + " do not have a mapping");
                    }
                }
                bw.write(curLine + "\n");
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


    public String[] getColumnName() {
        filePath = this.getClass().getClassLoader()
                .getResource((schema_origin)).getFile();
        String curLine;
        BufferedReader br = null;
        String columnName[] = new String[0];
        String splitLine[];
        String columnLine = "";
        try {
            br = new BufferedReader(new FileReader(filePath));
            while ((curLine = br.readLine()) != null) {
                splitLine = curLine.split("\t");
                columnLine += splitLine[0] + ",";
            }
            columnLine = columnLine.substring(0, columnLine.length() - 1);
            columnName = columnLine.split(",");
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
        return columnName;
    }


}
