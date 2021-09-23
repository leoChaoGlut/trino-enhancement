package personal.leo.trino.enhancement.function.impl;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import personal.leo.trino.enhancement.function.TrinoEnhancementFuntion;
import personal.leo.trino.enhancement.prop.TrinoProp;
import personal.leo.trino.enhancement.utils.TrinoSqlExecutor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class BuildSyncColumnsFunction implements TrinoEnhancementFuntion {
    private final String catalog;
    private final String schema;
    private final String table;
    private final TrinoSqlExecutor trinoSqlExecutor;

    public BuildSyncColumnsFunction(BuildSyncColumnsFunction.Input input) {
        this.catalog = input.getCatalog();
        this.schema = input.getSchema();
        this.table = input.getTable();
        this.trinoSqlExecutor = new TrinoSqlExecutor(input.getTrinoProp());
    }

    @Override
    public Object run() throws Exception {
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
        final String syncColumns = results.stream()
                .map(result -> {
                    final String columnName = (String) result.get(columnNameAlias);
                    return columnName;
                })
                .collect(Collectors.joining(","));
        log.info("SyncColumns: " + syncColumns);
        return syncColumns;
    }

    @Getter
    @Setter
    @Accessors(chain = true)
    @RequiredArgsConstructor
    public static class Input extends TrinoEnhancementFuntion.Input {
        private final TrinoProp trinoProp;
        private final String catalog;
        private final String schema;
        private final String table;

        public Input(TrinoProp trinoProp, String fullyQualifiedName) {
            this.trinoProp = trinoProp;
            final String[] strings = StringUtils.splitByWholeSeparator(fullyQualifiedName, ".");
            this.catalog = strings[0];
            this.schema = strings[1];
            this.table = strings[2];
        }
    }
}
