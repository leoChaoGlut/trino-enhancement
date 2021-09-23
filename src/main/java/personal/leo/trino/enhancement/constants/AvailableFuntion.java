package personal.leo.trino.enhancement.constants;

import lombok.AllArgsConstructor;
import lombok.Getter;
import personal.leo.trino.enhancement.function.TrinoEnhancementFuntion;
import personal.leo.trino.enhancement.function.impl.BuildSyncColumnsFunction;
import personal.leo.trino.enhancement.function.impl.ShardingSyncFunction;

@Getter
@AllArgsConstructor
public enum AvailableFuntion {

    ShardingSync(ShardingSyncFunction.Input.class),
    BuildSyncColumns(BuildSyncColumnsFunction.Input.class),
    ;

    private final Class<? extends TrinoEnhancementFuntion.Input> inputClass;
}
