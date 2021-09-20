package personal.leo.trinoShardingSync.service;

import org.junit.Before;
import org.junit.Test;
import personal.leo.trinoShardingSync.prop.TrinoProp;

public class TrinoSystemJdbcServiceTest {
    TrinoSystemJdbcService trinoSystemJdbcService;

    @Before
    public void before() {
        final TrinoProp trinoProp = new TrinoProp()
                .setUrl("jdbc:trino://localhost:10100");

        final String catalog = "hive";
        final String schema = "s1";
        final String table = "t1";

        trinoSystemJdbcService = new TrinoSystemJdbcService(trinoProp, catalog, schema, table);
    }

    @Test
    public void buildSyncColumns() {
        final String syncColumns = trinoSystemJdbcService.buildSyncColumns();
        System.out.println(syncColumns);
    }
}