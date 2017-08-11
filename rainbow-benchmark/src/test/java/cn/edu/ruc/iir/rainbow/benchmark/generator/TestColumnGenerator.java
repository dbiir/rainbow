package cn.edu.ruc.iir.rainbow.benchmark.generator;

import cn.edu.ruc.iir.rainbow.benchmark.util.SysSettings;
import cn.edu.ruc.iir.rainbow.benchmark.generator.ColumnGenerator;
import org.junit.jupiter.api.Test;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark
 * @ClassName: TestColumnGenerator
 * @Description: To Test functions of Class ColumnGenerator
 * @author: Tao
 * @date: Create in 2017-07-27 15:02
 **/
public class TestColumnGenerator {

    private ColumnGenerator columnGenerator = ColumnGenerator.Instance();

    @Test
    public void TestSetColumnShift() {
        columnGenerator.setColumnShift();
    }

    @Test
    public void TestFilePath() {
        String path = this.getClass().getClassLoader()
                .getResource((columnGenerator.schema_origin)).getFile();
        System.out.println("path is : " + path);
    }

    /**
     * @ClassName: TestColumnGenerator
     * @Title:
     * @Description: main method for the first task, for workload.txt & schema.txt
     * @param:
     * @date: 9:24 2017/7/29
     */
    @Test
    public void TestSetWorkloadShift() {
        columnGenerator.setColumnShift();
        columnGenerator.setWorkloadShift();
    }

    @Test
    public void TestGetColumnName() {
        SysSettings.CONFIG_DIRECTORY = "E:\\ruc_projects\\IntelliJHome\\rainbow\\rainbow-benchmark\\src\\main\\resources";

        long startTime = System.currentTimeMillis();
        String columnName[] = columnGenerator.getColumnName();
        long endTime = System.currentTimeMillis();
        System.out.println("Program run time : ： " + (endTime - startTime) + "ms");

        System.out.println(columnName.length);
    }


}
