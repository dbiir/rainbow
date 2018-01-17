package cn.edu.ruc.iir.rainbow.workload.eva;


import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;

import java.sql.*;
import java.util.Properties;

public class PrestoEvaluator {

    private static PrestoEvaluator evaluator = null;
    private static String user = null;
    private static String password = null;
    private static String ssl = null;
    private static Connection connection = null;

    private PrestoEvaluator() {
        Properties properties = new Properties();
        user = ConfigFactory.Instance().getProperty("presto.user");
        password = ConfigFactory.Instance().getProperty("presto.password");
        ssl = ConfigFactory.Instance().getProperty("presto.ssl");
        properties.setProperty("user", user);
        if (!password.equalsIgnoreCase("null")) {
            properties.setProperty("password", password);
        }
        properties.setProperty("SSL", ssl);
        String jdbcUrl = ConfigFactory.Instance().getProperty("presto.jdbc.url");
        try {
            Class.forName("com.facebook.presto.jdbc.PrestoDriver");
            connection = DriverManager.getConnection(jdbcUrl, properties);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static PrestoEvaluator Instance() {
        if (evaluator == null)
            evaluator = new PrestoEvaluator();
        return evaluator;
    }

    public void execute(String tableName, String columns, String orderByColumn) {

        String sql = "";
        try {
            Statement statement = connection.createStatement();
            if (orderByColumn == null) {
                sql = "select " + columns + " from " + tableName + " limit 10";
            } else {
                sql = "select " + columns + " from " + tableName + " order by " + orderByColumn + " limit 10";
            }
            ResultSet resultSet = statement.executeQuery(sql);
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            resultSet.next();
            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("SQL: " + sql);
        }

    }
}
