package io.github.yok.flexdblink.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration class that manages CLOB/BLOB file name patterns.<br>
 * Internally maintains a two-level map in the form of “table name → (column name → pattern
 * string)”.<br>
 * Provides safe accessors via {@link #getPattern(String, String)} and
 * {@link #getPatternsForTable(String)}.
 *
 * <p>
 * Typical usage is to bind YAML like:
 * </p>
 * 
 * <pre>
 * filePatterns:
 *   EMP:
 *     PHOTO: "{EMP_ID}.jpg"
 *     NOTE:  "{EMP_ID}_{SEQ}.txt"
 *   DOC:
 *     CONTENT: "{DOC_ID}.bin"
 * </pre>
 *
 * @author Yasuharu.Okawauchi
 */
@Component
@ConfigurationProperties
@Data
public class FilePatternConfig {

    /**
     * Map of “table name → (column name → file name pattern)”.
     */
    private Map<String, Map<String, String>> filePatterns = new HashMap<>();

    /**
     * Returns the file name pattern configured for the specified table and column.<br>
     * If the table or column does not exist, returns {@link Optional#empty()}.
     *
     * @param tableName table name (case-sensitive, must match the YAML definition)
     * @param columnName column name
     * @return an {@code Optional<String>} containing the configured pattern if present; empty
     *         otherwise
     */
    public Optional<String> getPattern(String tableName, String columnName) {
        Map<String, String> cols = filePatterns.get(tableName);
        if (cols == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(cols.get(columnName));
    }

    /**
     * Returns the full column-to-pattern map for the specified table.
     *
     * @param tableName table name
     * @return an unmodifiable map of “column name → pattern string”; empty if the table has no
     *         entry
     */
    public Map<String, String> getPatternsForTable(String tableName) {
        Map<String, String> cols = filePatterns.get(tableName);
        return cols != null ? Collections.unmodifiableMap(cols) : Collections.emptyMap();
    }
}
