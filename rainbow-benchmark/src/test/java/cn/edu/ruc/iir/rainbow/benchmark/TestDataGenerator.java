package cn.edu.ruc.iir.rainbow.benchmark;

import org.junit.jupiter.api.Test;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark
 * @ClassName: TestDataGenerator
 * @Description: To Test functions of Class DataGenerator
 * @author: Tao
 * @date: Create in 2017-07-27 15:02
 **/
public class TestDataGenerator {

    private DataGenerator dataGenerator = DataGenerator.Instance();

    @Test
    public void TestSetColumnShift() {
        dataGenerator.setColumnShift();
    }

    @Test
    public void TestFilePath() {
        String path = this.getClass().getClassLoader()
                .getResource((dataGenerator.schema_origin)).getFile();
        System.out.println("path is : " + path);
    }

    /**
     * @ClassName: TestDataGenerator
     * @Title:
     * @Description: main method for the first task, for workload.txt & schema.txt
     * @param:
     * @date: 9:24 2017/7/29
     */
    @Test
    public void TestSetWorkloadShift() {
        dataGenerator.setColumnShift();
        dataGenerator.setWorkloadShift();
    }


}
