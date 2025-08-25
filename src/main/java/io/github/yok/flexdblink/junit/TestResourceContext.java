package io.github.yok.flexdblink.junit;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.ClassPath;
import io.github.yok.flexdblink.config.ConnectionConfig;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import javax.sql.DataSource;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Shared test resource context for JUnit 5 extensions.
 *
 * <p>
 * Responsibilities:
 * </p>
 * <ul>
 * <li>Resolve the test-class resource root:
 * {@code src/test/resources/{package-path}/{TestClassName}}.</li>
 * <li>Resolve per-scenario {@code input/} and {@code expected/} directories.</li>
 * <li>List scenario names and DB IDs from the filesystem.</li>
 * <li>Load and merge {@code application.properties} and <strong>all</strong>
 * {@code application-*.properties} from the classpath using a deterministic order.</li>
 * <li>Build {@link ConnectionConfig.Entry} from merged properties.</li>
 * <li>Locate a Spring-managed {@link DataSource} that participates in the test transaction and
 * obtain a transactional {@link Connection} via {@link DataSourceUtils}.</li>
 * </ul>
 *
 * <h3>Properties loading and merge order (UTF-8, last-write-wins)</h3>
 * <ol>
 * <li>Load <em>all</em> {@code application.properties} found on the classpath (sorted by URL string
 * ascending).</li>
 * <li>Scan the classpath for <em>all</em> {@code application-*.properties} at any location.</li>
 * <li>Determine active profiles from {@code spring.profiles.active} in the merged properties,
 * overridden by the JVM system property if set (comma/semicolon separated).</li>
 * <li>Load <em>non-active</em> {@code application-*.properties} in ascending resource-name
 * order.</li>
 * <li>Load <em>active</em> {@code application-&lt;profile&gt;.properties} last, in the order listed
 * by {@code spring.profiles.active}; for each profile, multiple resources are loaded in ascending
 * resource-name order.</li>
 * </ol>
 */
@Slf4j
class TestResourceContext {

    @Getter
    private final Path classRoot; // src/test/resources/{package}/{TestClassName}

    @Getter
    private final Properties appProps; // merged application*.properties (UTF-8)

    private TestResourceContext(Path classRoot, Properties appProps) {
        this.classRoot = classRoot;
        this.appProps = appProps;
    }

    /**
     * Initialize the context from a JUnit {@link ExtensionContext}.
     *
     * @param ctx JUnit extension context (must not be null)
     * @return initialized {@link TestResourceContext}
     * @throws Exception if resource resolution or properties loading fails
     */
    static TestResourceContext init(@NonNull ExtensionContext ctx) throws Exception {
        Class<?> testClass = ctx.getRequiredTestClass();
        Path root = resolveTestClassRootFromClasspath(testClass);
        Properties props =
                loadAllApplicationProperties(ctx.getRequiredTestClass().getClassLoader());
        log.info("TestResourceContext initialized. classRoot={}, propertiesCount={}", root,
                props.size());
        return new TestResourceContext(root, props);
    }

    /**
     * Resolve {@code {classRoot}/{scenario}/input/{dbId}} or {@code {classRoot}/input/{dbId}} if
     * {@code scenario} is blank.
     *
     * @param scenario scenario name (nullable/blank = no scenario segment)
     * @param dbId database logical ID (folder name)
     * @return absolute, normalized path to the input directory
     */
    Path inputDir(String scenario, String dbId) {
        Path base = StringUtils.isBlank(scenario) ? classRoot : classRoot.resolve(scenario);
        return base.resolve("input").resolve(dbId).toAbsolutePath().normalize();
    }

    /**
     * Resolve {@code {classRoot}/{scenario}/expected/{dbId}} or {@code {classRoot}/expected/{dbId}}
     * if {@code scenario} is blank.
     *
     * @param scenario scenario name (nullable/blank = no scenario segment)
     * @param dbId database logical ID (folder name)
     * @return absolute, normalized path to the expected directory
     */
    Path expectedDir(String scenario, String dbId) {
        Path base = StringUtils.isBlank(scenario) ? classRoot : classRoot.resolve(scenario);
        return base.resolve("expected").resolve(dbId).toAbsolutePath().normalize();
    }

    /**
     * Resolve the base input directory used to detect DB IDs.
     *
     * @param scenario scenario name (nullable/blank allowed)
     * @return absolute path to {@code {classRoot}/{scenario?}/input}
     */
    Path baseInputDir(String scenario) {
        Path base = StringUtils.isBlank(scenario) ? classRoot : classRoot.resolve(scenario);
        return base.resolve("input").toAbsolutePath().normalize();
    }

    /**
     * Resolve the base expected directory used to detect DB IDs.
     *
     * @param scenario scenario name (nullable/blank allowed)
     * @return absolute path to {@code {classRoot}/{scenario?}/expected}
     */
    Path baseExpectedDir(String scenario) {
        Path base = StringUtils.isBlank(scenario) ? classRoot : classRoot.resolve(scenario);
        return base.resolve("expected").toAbsolutePath().normalize();
    }

    /**
     * List immediate subdirectory names under the given parent directory.
     *
     * @param parent parent directory path
     * @return sorted list of directory names; empty if {@code parent} is not a directory
     * @throws IOException if listing fails
     */
    List<String> listDirectories(Path parent) throws IOException {
        List<String> dirs = new ArrayList<>();
        if (!Files.isDirectory(parent)) {
            return dirs;
        }
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(parent, Files::isDirectory)) {
            for (Path p : ds) {
                dirs.add(p.getFileName().toString());
            }
        }
        dirs.sort(Comparator.naturalOrder());
        return dirs;
    }

    /**
     * Detect DB IDs (folder names) directly under the given base directory.
     *
     * @param baseDir base directory (e.g., {@code input/} or {@code expected/})
     * @return sorted list of DB IDs
     * @throws IOException if listing fails
     */
    List<String> detectDbIds(Path baseDir) throws IOException {
        return listDirectories(baseDir);
    }

    /**
     * Return a {@link DataSource} bound to Spring's test transaction if present.
     *
     * <p>
     * This checks {@link TransactionSynchronizationManager#getResourceMap()} and returns the first
     * key that is a {@link DataSource}.
     * </p>
     *
     * @return optional {@link DataSource} currently bound to the transaction
     */
    Optional<DataSource> springManagedDataSource() {
        Map<Object, Object> resourceMap = TransactionSynchronizationManager.getResourceMap();
        for (Map.Entry<Object, Object> e : resourceMap.entrySet()) {
            if (e.getKey() instanceof DataSource) {
                return Optional.of((DataSource) e.getKey());
            }
        }
        return Optional.empty();
    }

    /**
     * Obtain a JDBC {@link Connection} that participates in Spring's test transaction.
     *
     * <p>
     * Callers should not close the returned connection directly. Let Spring manage its lifecycle.
     * </p>
     *
     * @param dsOpt optional {@link DataSource} (usually from {@link #springManagedDataSource()})
     * @return optional transactional connection
     */
    Optional<Connection> springManagedConnection(Optional<DataSource> dsOpt) {
        if (dsOpt.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(DataSourceUtils.getConnection(dsOpt.get()));
    }

    /**
     * Build a {@link ConnectionConfig.Entry} from merged properties for the given logical DB name.
     *
     * <p>
     * Keys are selected by {@link #resolvePropertyForDb(Properties, String, String)} using a
     * scoring rule that prefers contiguous DB-name segments near the suffix.
     * </p>
     *
     * @param dbId logical database name (nullable/blank allowed)
     * @return populated entry; requires at least {@code *.url} and {@code *.username}
     * @throws IllegalStateException if required properties are missing
     */
    ConnectionConfig.Entry buildEntryFromProps(String dbId) {
        final String dbName = StringUtils.isBlank(dbId) ? null : dbId.trim();
        final String dbKeyLower = (dbName == null) ? null : dbName.toLowerCase(Locale.ROOT);

        final String url = resolvePropertyForDb(appProps, dbKeyLower, "url");
        final String user = resolvePropertyForDb(appProps, dbKeyLower, "username");
        final String pass = resolvePropertyForDb(appProps, dbKeyLower, "password");
        final String driver = resolvePropertyForDb(appProps, dbKeyLower, "driver-class-name");

        if (url == null || user == null) {
            String msg = "Missing connection properties for DB="
                    + (dbName == null ? "<default>" : dbName) + " (required: *.url and *.username)";
            throw new IllegalStateException(msg);
        }

        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId(dbName == null ? "default" : dbName);
        entry.setUrl(url);
        entry.setUser(user);
        entry.setPassword(pass);
        entry.setDriverClass(driver);
        return entry;
    }

    /**
     * Resolve a property value by suffix with DB-segment scoring.
     *
     * @param props merged properties
     * @param dbNameLower lowercase DB name (may be null for default)
     * @param suffix property suffix (e.g., {@code url}, {@code username})
     * @return resolved value or {@code null} if none found
     */
    private String resolvePropertyForDb(Properties props, String dbNameLower, String suffix) {
        final String targetSuffix = "." + suffix;
        String bestKey = null;
        int bestScore = Integer.MIN_VALUE;

        final String[] dbSegs =
                StringUtils.isBlank(dbNameLower) ? new String[0] : dbNameLower.split("\\.");

        for (Map.Entry<Object, Object> e : props.entrySet()) {
            if (!(e.getKey() instanceof String) || !(e.getValue() instanceof String)) {
                continue;
            }
            final String rawKey = (String) e.getKey();
            final String keyLower = rawKey.toLowerCase(Locale.ROOT);

            if (!keyLower.endsWith(targetSuffix)) {
                continue;
            }

            int score = scoreKeyWithSegments(keyLower, dbSegs, suffix);
            if (score > bestScore) {
                bestScore = score;
                bestKey = rawKey;
            }
        }
        return bestKey == null ? null : props.getProperty(bestKey);
    }

    /**
     * Score a property key higher when DB-name segments match contiguously and near the suffix.
     *
     * @param keyLower lowercase property key (e.g., {@code spring.datasource.bbb.url})
     * @param dbSegs DB-name segments (split by '.')
     * @param suffix property suffix (e.g., {@code url}, {@code username})
     * @return score (higher = better match)
     */
    private int scoreKeyWithSegments(String keyLower, String[] dbSegs, String suffix) {
        int score = 0;

        final String trimmed = keyLower.substring(0, keyLower.length() - (suffix.length() + 1));
        final String[] tokens = trimmed.split("\\.");
        final int tailIdx = tokens.length - 1;

        for (String t : tokens) {
            if ("datasource".equals(t)) {
                score += 1;
                break;
            }
        }

        boolean dbMatched = false;
        int matchEndIdx = -1;
        if (dbSegs.length > 0) {
            outer: for (int i = 0; i + dbSegs.length - 1 < tokens.length; i++) {
                for (int j = 0; j < dbSegs.length; j++) {
                    if (!tokens[i + j].equals(dbSegs[j])) {
                        continue outer;
                    }
                }
                dbMatched = true;
                matchEndIdx = i + dbSegs.length - 1;
                break;
            }
        }

        if (dbSegs.length == 0) {
            score += 50;
            if (tokens.length >= 2 && "spring".equals(tokens[0])
                    && "datasource".equals(tokens[1])) {
                score += 2;
            }
            return score;
        }

        if (dbMatched) {
            score += 100;
            if (matchEndIdx == tailIdx - 1) {
                score += 15;
            } else {
                int distance = Math.max(0, (tailIdx - 1) - matchEndIdx);
                score += (10 - Math.min(10, distance));
            }
            if (tokens.length >= 3 && "spring".equals(tokens[0])) {
                int firstIdx = matchEndIdx - (dbSegs.length - 1);
                if (firstIdx == 1 && "datasource".equals(tokens[2])) {
                    score += 3;
                }
                if (tokens.length >= 3 && "datasource".equals(tokens[1]) && firstIdx == 2) {
                    score += 2;
                }
            }
        } else {
            if (tokens.length >= 2 && "spring".equals(tokens[0])
                    && "datasource".equals(tokens[1])) {
                score += 2;
            }
        }
        return score;
    }

    /**
     * Load and merge <em>all</em> {@code application*.properties} from the classpath using the
     * documented order. Files are read as UTF-8; later loads overwrite earlier keys.
     *
     * @param cl class loader to use
     * @return merged {@link Properties}
     * @throws Exception if resource discovery or reading fails
     */
    private static Properties loadAllApplicationProperties(ClassLoader cl) throws Exception {
        Properties result = new Properties();

        // Load all application.properties (sorted by URL string)
        List<URL> baseUrls = enumToList(cl.getResources("application.properties"));
        baseUrls.sort(Comparator.comparing(URL::toString));
        for (URL u : baseUrls) {
            loadPropsUtf8(result, u);
            log.info("Loaded properties: {}", u);
        }

        // Discover all application-*.properties on the classpath
        Set<ClassPath.ResourceInfo> all = ClassPath.from(cl).getResources();
        Map<String, List<URL>> profileToUrls = new LinkedHashMap<>();
        for (ClassPath.ResourceInfo ri : all) {
            String name = ri.getResourceName();
            if (!name.endsWith(".properties")) {
                continue;
            }

            String simple = name.contains("/") ? name.substring(name.lastIndexOf('/') + 1) : name;
            if (!simple.startsWith("application-") || !simple.endsWith(".properties")) {
                continue;
            }

            String profile = extractProfile(simple);
            if (StringUtils.isBlank(profile)) {
                continue;
            } else {
                profileToUrls.computeIfAbsent(profile, k -> new ArrayList<>()).add(ri.url());
            }
        }

        // Determine active profiles
        List<String> actives = resolveActiveProfiles(result);

        // Load non-active application-*.properties
        if (!profileToUrls.isEmpty()) {
            Set<String> activeSet = new LinkedHashSet<>(actives);
            List<Res> nonActive = new ArrayList<>();
            for (Map.Entry<String, List<URL>> e : profileToUrls.entrySet()) {
                if (activeSet.contains(e.getKey())) {
                    continue;
                }
                for (URL u : e.getValue()) {
                    nonActive.add(new Res(e.getKey(), u));
                }
            }
            nonActive.sort(Comparator.comparing(r -> r.url.toString()));
            for (Res r : nonActive) {
                loadPropsUtf8(result, r.url);
                log.info("Loaded properties (non-active profile={}): {}", r.profile, r.url);
            }
        }

        // Load active application-<profile>.properties last, in listed order
        for (String ap : actives) {
            List<URL> urls = profileToUrls.getOrDefault(ap, List.of());
            List<URL> sorted = new ArrayList<>(urls);
            sorted.sort(Comparator.comparing(URL::toString));
            for (URL u : sorted) {
                loadPropsUtf8(result, u);
                log.info("Loaded properties (active profile={}): {}", ap, u);
            }
        }
        return result;
    }

    /**
     * Extract the profile token from a file name like {@code application-<profile>.properties}.
     *
     * @param simpleName simple resource name (no path)
     * @return extracted profile or empty string when not applicable
     */
    private static String extractProfile(String simpleName) {
        String mid = StringUtils.substringBetween(simpleName, "application-", ".properties");
        return mid == null ? "" : mid.trim();
    }

    /**
     * Resolve active profiles from merged properties and the JVM system property override.
     *
     * @param merged merged properties
     * @return immutable list of active profile names (possibly empty)
     */
    private static List<String> resolveActiveProfiles(Properties merged) {
        String sys = System.getProperty("spring.profiles.active");
        String val = StringUtils.isNotBlank(sys) ? sys
                : merged.getProperty("spring.profiles.active", "");
        if (StringUtils.isBlank(val)) {
            return List.of();
        }
        Iterable<String> split =
                Splitter.onPattern("[,;]").omitEmptyStrings().trimResults().split(val);
        List<String> out = new ArrayList<>();
        for (String s : split) {
            if (StringUtils.isNotBlank(s)) {
                out.add(s);
            }
        }
        return ImmutableList.copyOf(out);
    }

    /**
     * Load a {@code .properties} resource using UTF-8 into the target {@link Properties}. Existing
     * keys are overwritten.
     *
     * @param target target properties to mutate
     * @param url resource URL
     * @throws IOException if reading fails
     */
    private static void loadPropsUtf8(Properties target, URL url) throws IOException {
        try (InputStream in = new BufferedInputStream(url.openStream());
                InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
            Properties p = new Properties();
            p.load(reader);
            for (Map.Entry<Object, Object> e : p.entrySet()) {
                target.put(e.getKey(), e.getValue());
            }
        }
    }

    /**
     * Convert an {@link Enumeration} of URLs to a {@link List}.
     *
     * @param e enumeration
     * @return list containing all elements in order
     */
    private static List<URL> enumToList(Enumeration<URL> e) {
        List<URL> out = new ArrayList<>();
        while (e.hasMoreElements()) {
            out.add(e.nextElement());
        }
        return out;
    }

    /**
     * Resolve {@code src/test/resources/{package-path}/{TestClassName}} from the classpath.
     *
     * <p>
     * At test runtime, this folder is typically copied onto the classpath. This method locates it
     * via the context class loader.
     * </p>
     *
     * @param testClass test class
     * @return absolute, normalized path to the resource root
     * @throws Exception if the resource is not found or is not a directory
     */
    private static Path resolveTestClassRootFromClasspath(Class<?> testClass) throws Exception {
        String pkg = Optional.ofNullable(testClass.getPackage()).map(Package::getName).orElse("");
        String pkgPath = pkg.replace('.', '/');
        String resourcePath =
                (pkgPath.isEmpty() ? "" : (pkgPath + "/")) + testClass.getSimpleName();
        URL url = Thread.currentThread().getContextClassLoader().getResource(resourcePath);
        if (url == null) {
            String msg = "Test resource folder not found on classpath: " + resourcePath;
            throw new IllegalStateException(msg);
        }
        Path p = Paths.get(url.toURI()).toAbsolutePath().normalize();
        if (!Files.isDirectory(p)) {
            String msg = "Resolved test resource path is not a directory: " + p;
            throw new IllegalStateException(msg);
        }
        return p;
    }

    /**
     * Simple holder for a profile name and its resource URL.
     */
    private static final class Res {
        final String profile;
        final URL url;

        Res(String profile, URL url) {
            this.profile = Objects.requireNonNull(profile);
            this.url = Objects.requireNonNull(url);
        }
    }
}
