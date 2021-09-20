package personal.leo.trinoShardingSync.prop;

import io.trino.jdbc.TrinoDriver;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
@Getter
@Setter
public class TrinoProp {
    private String url;
    private String user = "root";
    private String driver = TrinoDriver.class.getName();
}
