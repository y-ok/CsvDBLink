package io.github.yok.csvdblink.config;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration class that holds settings related to dump operations.
 *
 * <p>
 * You can specify the following properties in {@code application.yml} or
 * {@code application.properties}.
 * </p>
 * <ul>
 * <li>{@code dump.excludeTables}: List of table names to exclude from the dump</li>
 * </ul>
 *
 * <p>
 * If not specified, all tables are included in the dump.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Component
@ConfigurationProperties(prefix = "dump")
@Getter
@Setter
@NoArgsConstructor
public class DumpConfig {

    /**
     * List of table names to exclude during dump processing.
     */
    private List<String> excludeTables = ImmutableList.of();

}
