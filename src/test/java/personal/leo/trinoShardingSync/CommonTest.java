package personal.leo.trinoShardingSync;

import org.junit.Test;

import java.util.regex.Pattern;

public class CommonTest {

    @Test
    public void regex() {
        final String regex = "^(hive[0-9]+)$";
        System.out.println(Pattern.matches(regex, "hive"));
        System.out.println(Pattern.matches(regex, "hive1"));
    }
}
