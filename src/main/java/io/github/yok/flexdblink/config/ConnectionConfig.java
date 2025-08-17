package io.github.yok.flexdblink.config;

import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration class that manages DB connection settings loaded from {@code connections.yml}.<br>
 * Parses the YAML file and maps it to Java objects.
 *
 * <pre>
 * connections:
 *   - id: db1
 *     url: jdbc:oracle:thin:@localhost:1521/XEPDB1
 *     user: testuser
 *     password: password
 *     driverClass: oracle.jdbc.OracleDriver
 * </pre>
 *
 * @author Yasuharu.Okawauchi
 */
@Component
@ConfigurationProperties
@Data
public class ConnectionConfig {

    /**
     * List of connection entries.
     */
    private List<Entry> connections;

    /**
     * Inner class that holds one DB connection setting.
     */
    @Data
    public static class Entry {
        // Logical ID of the target connection (e.g., "db1")
        private String id;
        // JDBC connection URL (e.g., jdbc:oracle:thin:@localhost:1521/XEPDB1)
        private String url;
        // Database user name
        private String user;
        // Database password
        private String password;
        // Fully qualified JDBC driver class name (e.g., oracle.jdbc.OracleDriver)
        private String driverClass;
    }
}
