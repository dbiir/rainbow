package cn.edu.ruc.iir.rainbow.web.hive.util;

import java.sql.*;

/**
 * rainbow
 *
 * @author guodong
 */
public class HiveClient {
    private final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private Connection conn;

    private static HiveClient instance = null;

    private HiveClient(String hostUrl, String userName, String passWord) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // todo exception handler
            e.printStackTrace();
        }
        try {
            conn = DriverManager.getConnection(hostUrl, userName, passWord);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static HiveClient Instance(String hostUrl, String userName, String passWord) {
        if (instance == null) {
            instance = new HiveClient(hostUrl, userName, passWord);
        }
        return instance;
    }

    public void execute(String statement) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(statement);
        } catch (SQLException e) {
            // todo exception handler
            e.printStackTrace();
        }
    }


    public ResultSet select(String statement) {
        ResultSet res = null;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            res = stmt.executeQuery(statement);
        } catch (SQLException e) {
            // todo exception handler
            e.printStackTrace();
        }
        return res;
    }

    public void drop(String table) {
        execute("DROP TABLE IF EXISTS " + table);
    }
}
