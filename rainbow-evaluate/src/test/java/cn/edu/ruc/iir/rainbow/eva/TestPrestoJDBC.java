package cn.edu.ruc.iir.rainbow.eva;

import org.junit.Test;

import java.sql.*;

public class TestPrestoJDBC
{
    @Test
    public void test () throws SQLException
    {
        String url = "jdbc:presto://presto00:8080/hive/rainbow";
        Connection connection = DriverManager.getConnection(url, "test", null);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) from orc");
        while (resultSet.next())
        {
            System.out.println(resultSet.getInt(1));
        }
    }
}
