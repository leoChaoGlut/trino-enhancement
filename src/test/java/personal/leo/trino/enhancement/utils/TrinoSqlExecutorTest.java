package personal.leo.trino.enhancement.utils;

import org.junit.Before;
import org.junit.Test;
import personal.leo.trino.enhancement.prop.TrinoProp;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TrinoSqlExecutorTest {

    TrinoSqlExecutor trinoSqlExecutor;


    @Before
    public void before() {
        final TrinoProp trinoProp = new TrinoProp("jdbc:trino://localhost:10100");

        trinoSqlExecutor = new TrinoSqlExecutor(trinoProp);
    }

    @Test
    public void fullyQualifiedNameWithDoubleQuote() {
        final String fullyQualifedName = "hive.s1.t2";
        System.out.println(trinoSqlExecutor.fullyQualifiedNameWithDoubleQuote(fullyQualifedName));
    }

    @Test
    public void testStatementClose() throws SQLException {
        final Connection connection = trinoSqlExecutor.createConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("select 1");
        statement.close();

        final Statement statement1 = connection.createStatement();
        final ResultSet resultSet1 = statement1.executeQuery("select 2");
        statement1.close();
    }

    @Test
    public void testConnectionClose() throws SQLException {
        final Connection connection = trinoSqlExecutor.createConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("select 1");
        statement.close();

        connection.close();

        final Statement statement1 = connection.createStatement();
        final ResultSet resultSet1 = statement1.executeQuery("select 2");
        statement1.close();
    }
}
