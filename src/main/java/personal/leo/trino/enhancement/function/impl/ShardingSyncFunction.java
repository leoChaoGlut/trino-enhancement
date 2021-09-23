package personal.leo.trino.enhancement.function.impl;

import com.alibaba.fastjson.JSON;
import lombok.*;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import personal.leo.trino.enhancement.function.TrinoEnhancementFuntion;
import personal.leo.trino.enhancement.prop.TrinoProp;
import personal.leo.trino.enhancement.utils.TrinoSqlExecutor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
public class ShardingSyncFunction implements TrinoEnhancementFuntion {
    @NonNull
    private final ShardingSyncFunction.Input input;
    private final Executor executor;

    public ShardingSyncFunction(ShardingSyncFunction.Input input) {
        this(input, null);
    }

    public ShardingSyncFunction(ShardingSyncFunction.Input input, Executor executor) {
        this.input = input;
        this.executor = executor == null ? Executors.newFixedThreadPool(input.getConcurrency()) : executor;
    }

    @Override
    public Object run() throws Exception {
        @Cleanup final TrinoSqlExecutor trinoSqlExecutor = new TrinoSqlExecutor(input.getTrinoProp());

        final String alias = "fullyQualifiedName";
        final String findMatchedTable = String.format("" +
                        "select table_cat || '.' || table_schem || '.' || table_name as " + alias + "\n" +
                        "from system.jdbc.tables\n" +
                        "where regexp_like(table_cat || '.' || table_schem || '.' || table_name, '%s')\n",
                input.getSrcFullyQualifiedNameRegex()
        );

        final List<Map<String, Object>> matchedFullyQualifedNames = trinoSqlExecutor.executeQuery(findMatchedTable);
        if (CollectionUtils.isEmpty(matchedFullyQualifedNames)) {
            throw new RuntimeException("No source table found: " + JSON.toJSONString(input));
        }

        final String sinkFullyQualifiedName = input.getSinkFullyQualifiedName();
        final String sinkFullyQualifiedNameWithDoubleQuote = trinoSqlExecutor.fullyQualifiedNameWithDoubleQuote(sinkFullyQualifiedName);
        final String whereClause = StringUtils.isBlank(input.getWhereClause()) ? "" : input.getWhereClause();

        final String syncColumns;
        if (StringUtils.isBlank(input.getSyncColumns())) {
            final BuildSyncColumnsFunction.Input input = new BuildSyncColumnsFunction.Input(this.input.getTrinoProp(), sinkFullyQualifiedName);
            syncColumns = (String) new BuildSyncColumnsFunction(input).run();
        } else {
            syncColumns = input.getSyncColumns();
        }

        final String[] syncColumnsArr = StringUtils.splitByWholeSeparator(syncColumns, ",");
        final String formattedSyncColumns = Arrays.stream(syncColumnsArr).map(syncColumn -> "\"" + syncColumn + "\"").collect(Collectors.joining("\n,"));

        final String insertIntoSelectSqlTmpl = "insert into " + sinkFullyQualifiedNameWithDoubleQuote + " (\n " +
                formattedSyncColumns +
                "\n)\n" +
                "select \n " + formattedSyncColumns + "\n" +
                "from %s\n" +
                whereClause;

        final List<CompletableFuture<Void>> futures = matchedFullyQualifedNames.stream()
                .map(matchedFullyQualifedName -> {
                    final String srcFullyQualifiedName = (String) matchedFullyQualifedName.get(alias);
                    final String srcFullyQualifiedNameWithDoubleQuote = trinoSqlExecutor.fullyQualifiedNameWithDoubleQuote(srcFullyQualifiedName);
                    final String insertIntoSelectSql = String.format(insertIntoSelectSqlTmpl, srcFullyQualifiedNameWithDoubleQuote);

                    final Runnable runnable = () -> {
                        final int count = trinoSqlExecutor.executeUpdate(insertIntoSelectSql);
                        log.info("Insert is finished: " + count);
                    };

                    return CompletableFuture.runAsync(runnable, executor);
                })
                .collect(Collectors.toList());

        for (CompletableFuture<Void> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                throw new RuntimeException("future.get() error: " + e);
            }
        }
        return null;
    }

    @Getter
    @Setter
    @Accessors(chain = true)
    @RequiredArgsConstructor
    public static class Input extends TrinoEnhancementFuntion.Input {
        @NonNull
        private final TrinoProp trinoProp;

        /**
         * e.g: ^(mysql[0-9]+).(test[0-9]+).(t[0-9]+)$
         */
        @NonNull
        private final String srcFullyQualifiedNameRegex;

        /**
         * e.g: catalog.schema.table
         */
        @NonNull
        private final String sinkFullyQualifiedName;

        /**
         * e.g: col1,col2,col3
         */
        @NonNull
        private final String syncColumns;

        /**
         * e.g: where create_time > timestamp '2021-01-01 01:01:01'
         */
        private String whereClause;

        private int concurrency = 1;
    }

}
