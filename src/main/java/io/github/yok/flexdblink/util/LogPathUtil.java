package io.github.yok.flexdblink.util;

import com.google.common.base.Preconditions;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility for rendering directory paths for logs.
 * <p>
 * If the given directory is under {@code $PROJECT_ROOT/target/test-classes}, this utility returns
 * the path relative to that base; otherwise, it returns the absolute normalized path.
 * </p>
 */
@Slf4j
public final class LogPathUtil {

    // Prevent instantiation
    private LogPathUtil() {
        throw new AssertionError("No io.github.yok.flexdblink.util.LogPathUtil instances for you!");
    }

    /**
     * // Render directory path for logs: // - If the given directory is under
     * {@code $PROJECT_ROOT/target/test-classes}, return the path relative to that base. // -
     * Otherwise, return the absolute normalized path. // // @param dir directory as {@link File}
     * // @return path string rendered for logs // @throws NullPointerException if {@code dir} is
     * {@code null}
     */
    public static String renderDirForLog(File dir) {
        Preconditions.checkNotNull(dir, "dir must not be null");

        // Determine the base: $PROJECT_ROOT/target/test-classes
        Path base = Paths.get(System.getProperty("user.dir"), "target", "test-classes")
                .toAbsolutePath().normalize();

        // Normalize the given directory as Path
        Path abs = dir.toPath().toAbsolutePath().normalize();

        // If under base, make it relative; otherwise keep absolute
        if (abs.startsWith(base)) {
            String rel = base.relativize(abs).toString();
            log.debug("Rendered relative log path. base={}, abs={}, rel={}", base, abs, rel);
            return rel;
        }

        String absolute = abs.toString();
        log.debug("Rendered absolute log path. abs={}", absolute);
        return absolute;
    }
}
