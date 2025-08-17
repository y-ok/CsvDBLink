package io.github.yok.flexdblink.junit;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple static registry for handing over application-provided {@link javax.sql.DataSource}
 * instances to test utilities (JUnit Extensions) at test execution time.
 *
 * <h2>Purpose</h2>
 * <ul>
 * <li>Inject a {@link DataSource} that your application constructed dynamically (e.g., a pool
 * already switched to a particular target) into the tool layer.</li>
 * <li>Offer an official entry point for supplying {@link DataSource} objects directly, without
 * relying on property-key naming conventions.</li>
 * </ul>
 *
 * <h2>How to use</h2>
 * <ol>
 * <li>Before tests start, call {@link #register(String, DataSource)} for a single entry or
 * {@link #registerAll(Map)} for multiple entries.</li>
 * <li>The tool obtains the matching {@link DataSource} via {@link #find(String)} using the
 * {@code dbName} and opens a connection.</li>
 * <li>After tests complete, call {@link #clear()} to wipe the in-memory registry.</li>
 * </ol>
 *
 * <h2>Thread-safety</h2>
 *
 * <p>
 * Internally backed by a {@link ConcurrentHashMap}, so concurrent access is supported.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DataSourceRegistry {

    // Thread-safe mapping between logical database name (dbName) and its DataSource instance
    private static final ConcurrentHashMap<String, DataSource> REGISTRY = new ConcurrentHashMap<>();

    /**
     * Registers a single {@link DataSource}.
     *
     * @param dbName logical connection ID (e.g., {@code operator.bbb}, {@code auto.ccc})
     * @param dataSource the {@link DataSource} to register
     * @throws NullPointerException if any argument is {@code null}
     */
    public static void register(String dbName, DataSource dataSource) {
        // Validate inputs to avoid null keys/values in the registry
        Objects.requireNonNull(dbName, "dbName");
        Objects.requireNonNull(dataSource, "dataSource");

        // Put or replace the mapping for the given dbName
        REGISTRY.put(dbName, dataSource);

        // Informational log for traceability during test setup
        log.info("register: dbName={}, dataSource={}", dbName, dataSource.getClass().getName());
    }

    /**
     * Registers multiple {@link DataSource} entries at once.
     *
     * @param map mapping of {@code dbName} to {@link DataSource}
     * @throws NullPointerException if {@code map} is {@code null}
     */
    public static void registerAll(Map<String, DataSource> map) {
        // Validate the input map before merging into the registry
        Objects.requireNonNull(map, "map");

        // Bulk insert/overwrite mappings
        REGISTRY.putAll(map);

        // Log size information to help diagnose test initialization
        log.info("registerAll: size={}", map.size());
    }

    /**
     * Finds a {@link DataSource} associated with the given {@code dbName}.
     *
     * @param dbName logical connection ID
     * @return an {@link Optional} containing the found {@link DataSource}, or empty if none
     */
    public static Optional<DataSource> find(String dbName) {
        // Return empty Optional for null keys to keep callers null-safe
        if (dbName == null) {
            return Optional.empty();
        }
        // Look up the mapping; returns Optional.empty() if no mapping exists
        return Optional.ofNullable(REGISTRY.get(dbName));
    }

    /**
     * Returns an immutable snapshot of all currently registered entries.
     *
     * @return an immutable copy of the current registry
     */
    public static Map<String, DataSource> snapshot() {
        // Create an immutable view using Guava for safe publication to callers
        return ImmutableMap.copyOf(REGISTRY);
    }

    /**
     * Unregisters a single mapping.
     *
     * @param dbName logical connection ID
     */
    public static void unregister(String dbName) {
        // No-op if null to keep callers simple and null-safe
        if (dbName == null) {
            return;
        }

        // Remove the mapping for the given dbName if present
        REGISTRY.remove(dbName);

        // Log the action for traceability in test teardown
        log.info("unregister: dbName={}", dbName);
    }

    /**
     * Clears all registered entries.
     */
    public static void clear() {
        // Remove all mappings to release any references to DataSource instances
        REGISTRY.clear();

        // Log the clear operation to aid debugging flaky tests or memory concerns
        log.info("clear");
    }
}
