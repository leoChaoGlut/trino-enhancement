package personal.leo.trino.enhancement.function.impl;

import org.junit.Test;
import personal.leo.trino.enhancement.prop.TrinoProp;

public class ShardingSyncFunctionTest {

    @Test
    public void run() throws Exception {
        final TrinoProp trinoProp = new TrinoProp("jdbc:trino://localhost:10100");

        final String srcFullyQualifiedNameRegex = "^(mysql).(test).(t[0-9]+)$";
        final String sinkFullyQualifiedName = "kudu.test.t";
        final String syncColumns = "c1,c2";
        final String whereClause = "where 1=1";
        final int concurrency = 1;

        final ShardingSyncFunction.Input input = new ShardingSyncFunction.Input(trinoProp, srcFullyQualifiedNameRegex, sinkFullyQualifiedName, syncColumns)
                .setWhereClause(whereClause)
                .setConcurrency(concurrency);

        new ShardingSyncFunction(input).run();
    }
}