package cn.edu.ruc.iir.rainbow.eva;

import cn.edu.ruc.iir.rainbow.eva.metrics.StageMetrics;

import java.sql.*;
import java.util.Properties;

public class PrestoEvaluator
{
    public static StageMetrics execute (String jdbcUrl, Properties jdbcProperties, String tableName, String columns, String orderByColumn)
    {
        StageMetrics stageMetrics = new StageMetrics();

        try
        {
            Thread.sleep(1000);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcProperties))
        {
            Statement statement = connection.createStatement();
            String sql = "select " + columns + " from " + tableName + " order by " + orderByColumn + " limit 10";
            long start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            resultSet.next();
            stageMetrics.setDuration(System.currentTimeMillis() - start);
            stageMetrics.setId(0);
            statement.close();
        } catch (SQLException e)
        {
            e.printStackTrace();
        }

        return stageMetrics;
    }
}
