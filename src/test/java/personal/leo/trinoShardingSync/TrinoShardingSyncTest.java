package personal.leo.trinoShardingSync;


import org.junit.Before;
import org.junit.Test;
import personal.leo.trinoShardingSync.prop.TrinoProps;
import personal.leo.trinoShardingSync.prop.TrinoShardingSyncProp;

import java.util.concurrent.Executors;

public class TrinoShardingSyncTest {
    TrinoShardingSync trinoShardingSync;

    @Before
    public void before() {
        final TrinoProps trinoProps = new TrinoProps()
                .setUrl("jdbc:trino://localhost:10100");

        final String srcFullyQualifiedNameRegex = "^(mysql).(test).(t[0-9]+)$";
        final String sinkFullyQualifiedName = "kudu.test.t";
        final String syncColumns = "c1,c2";
        final String whereClause = "where 1=1";

        final TrinoShardingSyncProp trinoShardingSyncProp = new TrinoShardingSyncProp(trinoProps, srcFullyQualifiedNameRegex, sinkFullyQualifiedName, whereClause, syncColumns);

        trinoShardingSync = new TrinoShardingSync(trinoShardingSyncProp, Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
    }

    @Test
    public void sync() {
        trinoShardingSync.sync();
    }
}