package personal.leo.trino.enhancement.function.impl;

import org.junit.Test;
import personal.leo.trino.enhancement.prop.TrinoProp;

public class BuildSyncColumnsFunctionTest {

    @Test
    public void run() throws Exception {
        final TrinoProp trinoProp = new TrinoProp("jdbc:trino://localhost:10100");

        final String catalog = "hive";
        final String schema = "s1";
        final String table = "t1";

        final BuildSyncColumnsFunction.Input input = new BuildSyncColumnsFunction.Input(trinoProp, catalog, schema, table);

        new BuildSyncColumnsFunction(input).run();
    }
}