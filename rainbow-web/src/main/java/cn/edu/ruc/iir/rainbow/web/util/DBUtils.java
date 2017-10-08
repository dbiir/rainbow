package cn.edu.ruc.iir.rainbow.web.util;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class DBUtils
{

    private static Logger log = Logger.getLogger(DBUtils.class);
    private static DBUtils instance = null;

    private static String DRIVER;
    private static String URL;
    private static String USERID;
    private static String USERPASSWORD;
    private static Connection conn = null;

    private DBUtils()
    {
        try
        {
            ConfigFactory config = ConfigFactory.Instance();
            DRIVER = config.getProperty("driver");
            URL = config.getProperty("url");
            USERID = config.getProperty("user");
            USERPASSWORD = config.getProperty("password");

            Class.forName(DRIVER);
            conn = DriverManager.getConnection(URL, USERID, USERPASSWORD);
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static DBUtils Instance()
    {
        if (instance == null)
        {
            instance = new DBUtils();
        }
        return instance;
    }

    public static void close(ResultSet rs, Statement st, Connection conn)
    {
        try
        {
            if (rs != null)
                rs.close();
            if (st != null)
                st.close();
            if (conn != null)
                conn.close();
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void close(Statement st, Connection conn)
    {
        try
        {
            if (st != null)
                st.close();
            if (conn != null)
                conn.close();
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static ResultSet Select(String SQL)
    {
        Statement statement = null;
        ResultSet rs = null;
        try
        {
            statement = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            rs = statement.executeQuery(SQL);
        } catch (Exception e)
        {
            log.error("Select from sql server error! errmsg:{}", e);
        }
        return rs;
    }

    public static void Execute(String SQL)
    {
        Statement statement = null;
        try
        {
            statement = conn.createStatement();
            statement.execute(SQL);
        } catch (Exception e)
        {
            log.error("Execute sql error! errmsg:{}", e);
        } finally
        {
            close(statement, conn);
        }
    }
}
