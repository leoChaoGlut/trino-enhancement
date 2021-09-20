package personal.leo.trinoShardingSync.utils;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import personal.leo.trinoShardingSync.prop.TrinoProp;

import java.io.Closeable;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class TrinoSqlExecutor implements Closeable {
    @Getter
    private final TrinoProp trinoProp;

    private Connection connection;

    public Connection createConnection() throws SQLException {
        final Properties properties = new Properties();
        properties.setProperty("user", trinoProp.getUser());
        return DriverManager.getConnection(trinoProp.getUrl(), properties);
    }

    public void execute(String catalog, String schema, Consumer<Statement> function) {
        final Connection connection = createOrReuseConnection();
        try (
                final Statement statement = connection.createStatement()
        ) {
            if (StringUtils.isNotBlank(catalog)) {
                connection.setCatalog(catalog);
            }
            if (StringUtils.isNotBlank(schema)) {
                connection.setSchema(schema);
            }
            function.accept(statement);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void execute(Consumer<Statement> function) {
        execute(null, null, function);
    }


    public List<Map<String, Object>> executeQuery(String sql) {
        log.info("executeQuery: \n" + sql);
        final Connection connection = createOrReuseConnection();
        try (
                final Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(sql);
        ) {
            return convertResultSet(resultSet);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean execute(String sql) {
        log.info("execute: \n" + sql);
        final Connection connection = createOrReuseConnection();
        try (
                final Statement statement = connection.createStatement();
        ) {
            return statement.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public int executeUpdate(String sql) {
        log.info("executeUpdate: \n" + sql);
        final Connection connection = createOrReuseConnection();
        try (
                final Statement statement = connection.createStatement();
        ) {
            return statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String fullyQualifiedNameWithDoubleQuote(String fullyQualifedName) {
        final String[] strings = StringUtils.splitByWholeSeparator(fullyQualifedName, ".");
        return Arrays.stream(strings)
                .map(s -> "\"" + s + "\"")
                .collect(Collectors.joining("."));
    }

    @Override
    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection createOrReuseConnection() {
        if (connection == null) {
            synchronized (this) {
                try {
                    connection = createConnection();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return connection;
    }

    private List<Map<String, Object>> convertResultSet(ResultSet resultSet) throws SQLException {
        final List<Map<String, Object>> resultList = new ArrayList<>();
        while (resultSet.next()) {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();

            final Map<String, Object> resultMap = new HashMap<>();
            for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                final String columnName = metaData.getColumnName(columnIndex);
                final Object columnValue = resultSet.getObject(columnIndex);

                resultMap.put(columnName, columnValue);
            }
            resultList.add(resultMap);
        }
        return resultList;
    }
}
