package cn.edu.ruc.iir.rainbow.parser.sql;

import cn.edu.ruc.iir.rainbow.parser.sql.parser.ParsingOptions;
import cn.edu.ruc.iir.rainbow.parser.sql.parser.SqlParser;
import cn.edu.ruc.iir.rainbow.parser.sql.tree.CreateTable;
import cn.edu.ruc.iir.rainbow.parser.sql.tree.QualifiedName;
import cn.edu.ruc.iir.rainbow.parser.sql.tree.Query;
import cn.edu.ruc.iir.rainbow.parser.sql.tree.Statement;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SqlParserTest
{
    private SqlParser parser = new SqlParser();

    @Test
    public void testCreateSchema()
    {
        String sql = "CREATE SCHEMA IF NOT EXISTS test";
        Statement statement = parser.createStatement(sql);
//        CreateSchema expected = new CreateSchema(QualifiedName.of("test"), true);
//        System.out.println(statement.toString());
//        assertEquals(statement.toString(), expected.toString());
    }

    @Test
    public void testDropSchema()
    {
        String sql = "Drop SCHEMA IF EXISTS test";
        Statement statement = parser.createStatement(sql);
        System.out.println(statement.toString());
    }

    @Test
    public void testUse()
    {
        String sql = "use schema";
        Statement statement = parser.createStatement(sql);
        System.out.println(statement.toString());
    }


    @Test
    public void testShowTable()
    {
        String sql = "show tables";
        Statement statement = parser.createStatement(sql);
        // error
        Query q = (Query) statement;
        System.out.println(statement.toString());
    }

    @Test
    public void testCreateTableVP()
    {
        String sql =
                "CREATE TABLE orders_range\n" +
                "(id INT PRIMARY KEY, name VARCHAR(30)) ON node1,\n" +
                "(id INT PRIMARY KEY, order_date DATE)";
        Statement statement = parser.createStatement(sql);
        System.out.println(statement.toString());
    }

    @Test
    public void testCreateTableHPHash()
    {
        String sql =
                "CREATE TABLE orders_range\n" +
                        "(\n" +
                        "id INT PRIMARY KEY,\n" +
                        "name VARCHAR(30),\n" +
                        "order_date DATE\n" +
                        ") PARTITION BY HASH(id) PARTITIONS 4";
        Statement statement = parser.createStatement(sql);
        CreateTable ctstmt = (CreateTable) statement;
        QualifiedName name = ctstmt.getName();
        System.out.println("prefix:" + (name.getPrefix().isPresent() ? name.getPrefix().get() : null));
        System.out.println("suffix:" + name.getSuffix());
        System.out.println("orignal part:");
        name.getOriginalParts().forEach(System.out::println);
        System.out.println(statement.toString());
    }

    @Test
    public void testCreateTableHPList()
    {
        String sql =
                "CREATE TABLE orders_range\n" +
                "(\n" +
                "id INT PRIMARY KEY,\n" +
                "name VARCHAR(30),\n" +
                "order_date DATE\n" +
                ") PARTITION BY LIST\n" +
                "(\n" +
                "p0 id< IN (1, 2, 3) AND name IN ('alice', 'bob') ON node0,\n" +
                "p1 id IN (4, 5) ON node1,\n" +
                "p2 id IN (8, 9, 10)\n" +
                ")";
        Statement statement = parser.createStatement(sql);
        System.out.println(statement.toString());
    }

    @Test
    public void testCreateTableHPRange()
    {
        String sql =
                "CREATE TABLE orders_range\n" +
                "(\n" +
                "id INT PRIMARY KEY,\n" +
                "name VARCHAR(30),\n" +
                "order_date DATE\n" +
                ") PARTITION BY RANGE\n" +
                "(\n" +
                "p0 id < 5 AND order_date >= '2017-01-01' ON node0,\n" +
                "p1 id < 10 ON node1,\n" +
                "p2 id < 15 ON node2,\n" +
                "p3 id < MAXVALUE\n" +
                ")";
        Statement statement = parser.createStatement(sql);
        System.out.println(statement.toString());
    }

    @Test
    public void testSelectSimple()
    {
        String sql =
                "SELECT * FROM test WHERE id > 10 GROUP BY name ORDER BY order_date LIMIT 10";
        Statement statement = parser.createStatement(sql);
        System.out.println(statement.toString());
    }

    @Test
    public void testSelectJoin()
    {
        String sql =
                "SELECT id, name FROM test0 JOIN test1 ON test0.id=test1.id where test0.id>10";
        Statement statement = parser.createStatement(sql);
        System.out.println(statement.toString());
    }

    @Test
    public void testInsert()
    {
        String sql =
                "INSERT INTO test VALUES(10, abc, 10.0)";
        ParsingOptions pOption = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);
        Statement statement = parser.createStatement(sql, pOption);
        System.out.println(statement.toString());
    }
}
