package personal.leo.trinoShardingSync.prop;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
@AllArgsConstructor
public class TrinoShardingSyncProp {
    @NonNull
    private TrinoProp trinoProp;

    /**
     * e.g: ^(mysql[0-9]+).(test[0-9]+).(t[0-9]+)$
     */
    @NonNull
    private String srcFullyQualifiedNameRegex;

    /**
     * e.g: catalog.schema.table
     */
    @NonNull
    private String sinkFullyQualifiedName;

    /**
     * e.g: where create_time > timestamp '2021-01-01 01:01:01'
     */
    private String whereClause;
    /**
     * e.g: col1,col2,col3
     */
    @NonNull
    private String syncColumns;
}
