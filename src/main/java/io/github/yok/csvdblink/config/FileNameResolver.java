package io.github.yok.csvdblink.config;

import com.google.common.base.Strings;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * Utility class that dynamically resolves file names for large text/binary columns such as
 * CLOB/BLOB.
 *
 * <p>
 * If a file name pattern is configured per table/column in {@link FilePatternConfig}, this class
 * expands placeholder variables (e.g., {@code {id}}) using the provided values. If no pattern is
 * configured, it falls back to the default naming scheme: {@code 
 * 
<table>
 * _<pk1>_<pk2>_..._<column>.<ext>}.
 * </p>
 *
 * <p>
 * This class performs <strong>no I/O</strong>; it only generates file name strings.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@RequiredArgsConstructor
public class FileNameResolver {

    // Holder for the file name pattern configuration
    private final FilePatternConfig patternConfig;

    /**
     * Determines the file name for a CLOB/BLOB (or other LOB) column.
     *
     * @param table table name
     * @param column column name
     * @param values map of values to embed into the pattern (e.g., primary keys), as
     *        {@code {columnName:value}}
     * @param ext file extension (e.g., {@code "bin"}, {@code "txt"}, {@code "jpg"}, etc.)
     * @return resolved file name; expands a configured pattern if present, otherwise uses the
     *         default naming scheme
     */
    public String resolve(String table, String column, Map<String, Object> values, String ext) {
        String pattern = null;
        if (patternConfig != null && patternConfig.getFilePatterns() != null
                && patternConfig.getFilePatterns().containsKey(table)) {
            pattern = patternConfig.getFilePatterns().get(table).get(column);
        }
        // If a pattern exists, replace variables like {id} with actual values
        if (StringUtils.isNotBlank(pattern)) {
            String filename = pattern;
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                filename = filename.replace("{" + entry.getKey() + "}",
                        String.valueOf(entry.getValue()));
            }
            return filename;
        }
        // When no pattern is defined, fallback to: <table>_<pk...>_<column>.<ext>
        StringBuilder base = new StringBuilder(table);
        for (String key : values.keySet()) {
            base.append("_").append(values.get(key));
        }
        return String.format("%s_%s.%s", base, column, Strings.nullToEmpty(ext));
    }
}
