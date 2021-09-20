package personal.leo.trinoShardingSync.service;

import lombok.extern.slf4j.Slf4j;
import personal.leo.trinoShardingSync.prop.TrinoProp;
import personal.leo.trinoShardingSync.utils.TrinoSqlExecutor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class TrinoSystemJdbcService {
    private final String catalog;
    private final String schema;
    private final String table;
    private final TrinoSqlExecutor trinoSqlExecutor;

    public TrinoSystemJdbcService(TrinoProp trinoProp, String catalog, String schema, String table) {
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.trinoSqlExecutor = new TrinoSqlExecutor(trinoProp);
    }

    public String buildSyncColumns() {
        final String columnNameAlias = "column_name";
        final String sql = String.format("" +
                        "select " + columnNameAlias + "\n" +
                        "from system.jdbc.columns\n" +
                        "where table_cat = '%s'\n" +
                        "  and table_schem = '%s'\n" +
                        "  and table_name = '%s'\n",
                catalog,
                schema,
                table
        );
        final List<Map<String, Object>> results = trinoSqlExecutor.executeQuery(sql);
        return results.stream()
                .map(result -> {
                    final String columnName = (String) result.get(columnNameAlias);
                    return columnName;
                })
                .collect(Collectors.joining(","));
    }

}
