package personal.leo.trinoShardingSync.service;


import org.junit.Before;
import org.junit.Test;
import personal.leo.trinoShardingSync.prop.TrinoProps;
import personal.leo.trinoShardingSync.prop.TrinoShardingSyncProp;

public class TrinoShardingSyncServiceTest {
    TrinoShardingSyncService trinoShardingSyncService;

    @Before
    public void before() {
        final TrinoProps trinoProps = new TrinoProps()
                .setUrl("jdbc:trino://localhost:10100");

        final String srcFullyQualifiedNameRegex = "^(mysql).(test).(t[0-9]+)$";
        final String sinkFullyQualifiedName = "kudu.test.t";
        final String syncColumns = "c1,c2";
        final String whereClause = "where 1=1";

        final TrinoShardingSyncProp trinoShardingSyncProp = new TrinoShardingSyncProp(trinoProps, srcFullyQualifiedNameRegex, sinkFullyQualifiedName, whereClause, syncColumns);

        trinoShardingSyncService = new TrinoShardingSyncService(trinoShardingSyncProp, 1);
    }

    @Test
    public void sync() {
        trinoShardingSyncService.sync();
    }
}