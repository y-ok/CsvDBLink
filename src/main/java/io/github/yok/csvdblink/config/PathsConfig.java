package io.github.yok.csvdblink.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration class that reads the {@code data-path} property from the application root
 * configuration and composes absolute directory paths for data loading (load) and data dumping
 * (dump).
 *
 * <p>
 * The {@code data-path} must point to the base directory under which this tool expects
 * {@code /load} and {@code /dump} subdirectories.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Component
@ConfigurationProperties
@Data
public class PathsConfig {

    // Base path that serves as the application's root data directory
    private String dataPath;

    /**
     * Returns the absolute path for the data loading directory.
     *
     * @return the path to the load directory
     * @throws IllegalStateException if {@code dataPath} has not been set
     */
    public String getLoad() {
        if (dataPath == null || dataPath.isBlank()) {
            throw new IllegalStateException(
                    "data-path is not configured. Please set 'data-path' in application.yml.");
        }
        return dataPath.endsWith("/") ? dataPath + "load" : dataPath + "/load";
    }

    /**
     * Returns the absolute path for the data dumping directory.
     *
     * @return the path to the dump directory
     * @throws IllegalStateException if {@code dataPath} has not been set
     */
    public String getDump() {
        if (dataPath == null || dataPath.isBlank()) {
            throw new IllegalStateException(
                    "data-path is not configured. Please set 'data-path' in application.yml.");
        }
        return dataPath.endsWith("/") ? dataPath + "dump" : dataPath + "/dump";
    }
}
