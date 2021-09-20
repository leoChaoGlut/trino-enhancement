package personal.leo.trinoShardingSync;

import com.alibaba.fastjson.JSON;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import personal.leo.trinoShardingSync.prop.TrinoShardingSyncProp;
import personal.leo.trinoShardingSync.utils.TrinoSqlExecutor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
public class TrinoShardingSync {
    @NonNull
    private final TrinoShardingSyncProp trinoShardingSyncProp;
    private final Executor executor;

    public TrinoShardingSync(TrinoShardingSyncProp trinoShardingSyncProp, int concurrency) {
        this(trinoShardingSyncProp, concurrency, null);
    }

    public TrinoShardingSync(TrinoShardingSyncProp trinoShardingSyncProp, Executor executor) {
        this(trinoShardingSyncProp, 1, executor);
    }

    private TrinoShardingSync(TrinoShardingSyncProp trinoShardingSyncProp, int concurrency, Executor executor) {
        this.trinoShardingSyncProp = trinoShardingSyncProp;
        this.executor = executor == null ? Executors.newFixedThreadPool(concurrency) : executor;
    }


    public void sync() {
        @Cleanup final TrinoSqlExecutor trinoSqlExecutor = new TrinoSqlExecutor(trinoShardingSyncProp.getTrinoProps());

        final String alias = "fullyQualifiedName";
        final String findMatchedTable = String.format("" +
                        "select table_cat || '.' || table_schem || '.' || table_name as " + alias + "\n" +
                        "from system.jdbc.tables\n" +
                        "where regexp_like(table_cat || '.' || table_schem || '.' || table_name, '%s')\n",
                trinoShardingSyncProp.getSrcFullyQualifiedNameRegex()
        );

        final List<Map<String, Object>> matchedFullyQualifedNames = trinoSqlExecutor.executeQuery(findMatchedTable);
        if (CollectionUtils.isEmpty(matchedFullyQualifedNames)) {
            throw new RuntimeException("No source table found: " + JSON.toJSONString(trinoShardingSyncProp));
        }

        final String sinkFullyQualifiedName = trinoShardingSyncProp.getSinkFullyQualifiedName();
        final String sinkFullyQualifiedNameWithDoubleQuote = trinoSqlExecutor.fullyQualifiedNameWithDoubleQuote(sinkFullyQualifiedName);
        final String whereClause = StringUtils.isBlank(trinoShardingSyncProp.getWhereClause()) ? "" : trinoShardingSyncProp.getWhereClause();

        final String[] syncColumnsArr = StringUtils.splitByWholeSeparator(trinoShardingSyncProp.getSyncColumns(), ",");
        final String syncColumns = Arrays.stream(syncColumnsArr).map(syncColumn -> "\"" + syncColumn + "\"").collect(Collectors.joining("\n,"));

        final String insertIntoSelectSqlTmpl = "insert into " + sinkFullyQualifiedNameWithDoubleQuote + " (\n " +
                syncColumns +
                "\n)\n" +
                "select \n " + syncColumns + "\n" +
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
    }

}
