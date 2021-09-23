package personal.leo.trino.enhancement.prop;

import io.trino.jdbc.TrinoDriver;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
@Getter
@Setter
@RequiredArgsConstructor
public class TrinoProp {
    private final String url;
    private String user = "root";
    private String driver = TrinoDriver.class.getName();
}
