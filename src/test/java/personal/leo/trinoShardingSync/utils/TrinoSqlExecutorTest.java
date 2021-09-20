package personal.leo.trinoShardingSync.utils;

import org.junit.Before;
import org.junit.Test;
import personal.leo.trinoShardingSync.prop.TrinoProps;

public class TrinoSqlExecutorTest {

    TrinoSqlExecutor trinoSqlExecutor;


    @Before
    public void before() {
        final TrinoProps trinoProps = new TrinoProps()
                .setUrl("jdbc:trino://localhost:10100");

        trinoSqlExecutor = new TrinoSqlExecutor(trinoProps);
    }

    @Test
    public void fullyQualifiedNameWithDoubleQuote() {
        final String fullyQualifedName = "hive.s1.t2";
        System.out.println(trinoSqlExecutor.fullyQualifiedNameWithDoubleQuote(fullyQualifedName));
    }
}
