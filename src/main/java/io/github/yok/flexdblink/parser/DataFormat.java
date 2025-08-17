package io.github.yok.flexdblink.parser;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Enumeration of supported data formats.
 *
 * <p>
 * Each format defines one or more file extensions that are recognized as belonging to that format.
 * For example, {@link #YAML} supports both {@code .yaml} and {@code .yml}.
 * </p>
 *
 * <p>
 * This enum is responsible for handling extension matching in a centralized way, so that other
 * classes (such as factories) do not need to hardcode string comparisons.
 * </p>
 */
@Getter
@AllArgsConstructor
public enum DataFormat {

    // Comma-Separated Values format (CSV).
    CSV("csv"),

    // JavaScript Object Notation format (JSON).
    JSON("json"),

    // YAML Ain't Markup Language format (YAML/YML).
    YAML("yaml", "yml"),

    // Extensible Markup Language format (XML).
    XML("xml");

    // Set of valid extensions for this format (all lowercase).
    private final Set<String> extensions;

    DataFormat(String... exts) {
        this.extensions = Arrays.stream(exts).map(String::toLowerCase).collect(Collectors.toSet());
    }

    /**
     * Determines whether the given file extension belongs to this format.
     *
     * @param ext file extension to check (case-insensitive, without dot)
     * @return {@code true} if the extension matches this format, {@code false} otherwise
     */
    public boolean matches(String ext) {
        return extensions.contains(ext.toLowerCase());
    }
}
