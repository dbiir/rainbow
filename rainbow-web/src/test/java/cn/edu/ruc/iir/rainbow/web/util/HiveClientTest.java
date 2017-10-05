package cn.edu.ruc.iir.rainbow.web.util;

import cn.edu.ruc.iir.rainbow.web.hive.util.HiveClient;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * rainbow
 *
 * @author guodong
 */
public class HiveClientTest {

    @Test
    public void HiveClientTest() {
        String hostUrl = "jdbc:hive2://10.77.40.236:10000/default";
        String userName = "presto";
        String passWord = "";
        HiveClient client = HiveClient.Instance(hostUrl, userName, passWord);

        String statement = "DROP TABLE IF EXISTS jdbc_test";
        client.execute(statement);

        statement = "CREATE TABLE jdbc_test(viewTime INT, userid BIGINT) PARTITIONED BY(dt STRING) STORED AS SEQUENCEFILE";
        client.execute(statement);
    }


    @Test
    public void SelectTest() {
        String hostUrl = "jdbc:hive2://10.77.40.236:10000/default";
        String userName = "presto";
        String passWord = "";
        HiveClient client = HiveClient.Instance(hostUrl, userName, passWord);

        String statement = "select Count(*) from parquet_7691bce468db2b3853ae2a9260c71506";
        ResultSet res = client.select(statement);
        try {
            while (res.next()) {
                System.out.println(res.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
