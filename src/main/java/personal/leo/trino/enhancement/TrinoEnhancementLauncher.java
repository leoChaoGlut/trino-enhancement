package personal.leo.trino.enhancement;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import personal.leo.trino.enhancement.constants.AvailableFuntion;
import personal.leo.trino.enhancement.function.TrinoEnhancementFuntion;
import personal.leo.trino.enhancement.function.impl.BuildSyncColumnsFunction;
import personal.leo.trino.enhancement.function.impl.ShardingSyncFunction;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;

public class TrinoEnhancementLauncher {
    public static void main(String[] args) throws Exception {
        final String function = args[0];
        final String jsonConfigFilePath = args[1];
        final String jsonConfigFileContent = IOUtils.toString(new FileInputStream(jsonConfigFilePath), StandardCharsets.UTF_8);
        final AvailableFuntion availableFuntion = AvailableFuntion.valueOf(StringUtils.lowerCase(function));
        final TrinoEnhancementFuntion.Input input = JSON.parseObject(jsonConfigFileContent, availableFuntion.getInputClass());

        switch (availableFuntion) {
            case ShardingSync:
                new ShardingSyncFunction((ShardingSyncFunction.Input) input).run();
                break;
            case BuildSyncColumns:
                new BuildSyncColumnsFunction((BuildSyncColumnsFunction.Input) input).run();
                break;
            default:
                throw new RuntimeException("Not supported function: " + function);
        }

        System.exit(0);
    }
}
