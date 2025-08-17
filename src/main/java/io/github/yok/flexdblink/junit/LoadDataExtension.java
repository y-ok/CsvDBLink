package io.github.yok.flexdblink.junit;

import com.google.common.collect.ImmutableList;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.core.DataLoader;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.OracleDateTimeFormatUtil;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * JUnit 5 extension that interprets {@link LoadData} and loads test CSV/LOB data.
 *
 * <h2>Features</h2>
 * <ul>
 * <li>Load timing: {@link #beforeTestExecution(ExtensionContext)} (after Spring's test transaction
 * starts).</li>
 * <li>Obtain a {@code DataSource} bound in Spring's test transaction via
 * {@link org.springframework.transaction.support.TransactionSynchronizationManager}, then acquire a
 * {@code
 * Connection} with
 * {@link org.springframework.jdbc.datasource.DataSourceUtils#getConnection(javax.sql.DataSource)}
 * so the load participates in the <b>test transaction</b>.</li>
 * <li>When using a Spring-managed connection, wrap it with a proxy that neutralizes {@code close()}
 * to avoid premature closing by third-party code.</li>
 * <li>Eliminates CLI dependency by overriding {@link PathsConfig} via an anonymous class.</li>
 * </ul>
 *
 * <h2>Directory layout</h2>
 * 
 * <pre>
 * src/test/resources/&lt;package-path&gt;/&lt;TestClassName&gt;/&lt;scenario&gt;/&lt;DB name&gt;/
 * </pre>
 *
 * <h2>Transaction policy</h2>
 * <ul>
 * <li>Principle: <b>delegate transaction control to the caller (test/app)</b></li>
 * <li>When participating in Spring: this class does not call commit/rollback/close. The test's
 * {@code @Transactional} rollback cleans up.</li>
 * </ul>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public class LoadDataExtension implements BeforeAllCallback, BeforeTestExecutionCallback {

    // The resources-root folder for the test class
    private Path testClassRoot;

    // application.properties merged with the active profile
    private Properties appProps;

    private static final String EXCLUDE_FLYWAY_SCHEMA_HISTORY = "flyway_schema_history";

    /**
     * Class-start: resolve paths and load properties only.
     *
     * @param context JUnit execution context
     * @throws Exception if resource resolution or properties loading fails
     */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        Class<?> testClass = context.getRequiredTestClass();
        this.testClassRoot = resolveTestClassRoot(testClass);
        this.appProps = loadAppProps();
    }

    /**
     * Just before each test executes: interpret {@link LoadData} on the class/method and perform
     * the load.
     *
     * <p>
     * This timing occurs after Spring's test transaction starts, so we participate if possible.
     * </p>
     *
     * @param context JUnit execution context
     * @throws Exception if loading fails
     */
    @Override
    public void beforeTestExecution(ExtensionContext context) throws Exception {
        if (testClassRoot == null || appProps == null) {
            beforeAll(context);
        }

        Class<?> testClass = context.getRequiredTestClass();

        LoadData classAnn = testClass.getAnnotation(LoadData.class);
        if (classAnn != null) {
            for (String scenario : resolveScenarios(classAnn)) {
                Path dir = testClassRoot.resolve(scenario);
                if (Files.isDirectory(dir)) {
                    loadScenarioParticipating(context, scenario, classAnn.dbNames());
                } else {
                    log.warn("Class-level scenario not found: {}", dir);
                }
            }
        }

        context.getTestMethod().ifPresent(m -> {
            LoadData methodAnn = m.getAnnotation(LoadData.class);
            if (methodAnn == null) {
                return;
            }
            try {
                for (String scenario : resolveScenarios(methodAnn)) {
                    Path dir = testClassRoot.resolve(scenario);
                    if (Files.isDirectory(dir)) {
                        loadScenarioParticipating(context, scenario, methodAnn.dbNames());
                    } else {
                        log.info("Scenario for method [{}] not found: {}", m.getName(), dir);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Load a given scenario.<br>
     * Always join the Spring-managed test transaction.
     *
     * @param context JUnit execution context (used to obtain Spring ApplicationContext)
     * @param scenarioName scenario directory name
     * @param dbNamesAttr DB names from the annotation (if omitted, auto-detect)
     * @throws Exception on read/load failure
     */
    private void loadScenarioParticipating(ExtensionContext context, String scenarioName,
            String[] dbNamesAttr) throws Exception {

        Path dumpRoot = Paths.get(System.getProperty("user.dir"), "target", "dbunit", "dump")
                .toAbsolutePath().normalize();
        Files.createDirectories(dumpRoot);

        CsvDateTimeFormatProperties dtProps = new CsvDateTimeFormatProperties();
        dtProps.setDate("yyyy-MM-dd");
        dtProps.setTime("HH:mm:ss");
        dtProps.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        dtProps.setDateTime("yyyy-MM-dd HH:mm:ss");
        OracleDateTimeFormatUtil dateTimeUtil = new OracleDateTimeFormatUtil(dtProps);

        PathsConfig pathsConfig = new PathsConfig() {
            @Override
            public String getLoad() {
                return testClassRoot.toAbsolutePath().toString();
            }

            @Override
            public String getDataPath() {
                return testClassRoot.toAbsolutePath().toString();
            }

            @Override
            public String getDump() {
                return dumpRoot.toString();
            }
        };

        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DumpConfig dumpConfig = new DumpConfig();
        List<String> merged = new ArrayList<>(
                Optional.ofNullable(dumpConfig.getExcludeTables()).orElse(List.of()));
        if (merged.stream().noneMatch(s -> EXCLUDE_FLYWAY_SCHEMA_HISTORY.equalsIgnoreCase(s))) {
            merged.add(EXCLUDE_FLYWAY_SCHEMA_HISTORY);
        }
        dumpConfig.setExcludeTables(ImmutableList.copyOf(merged));

        DbUnitConfigFactory configFactory = new DbUnitConfigFactory();
        ConnectionConfig connectionConfig = new ConnectionConfig();
        DbDialectHandlerFactory handlerFactory = new DbDialectHandlerFactory(dbUnitConfig,
                dumpConfig, pathsConfig, dateTimeUtil, configFactory);

        DataLoader loader = new DataLoader(pathsConfig, connectionConfig,
                entry -> Optional.ofNullable(entry.getUser()).map(u -> u.toUpperCase(Locale.ROOT))
                        .orElseThrow(() -> new IllegalStateException(
                                "Failed to resolve schema: user is undefined")),
                handlerFactory::create, dbUnitConfig, configFactory, dumpConfig);

        // DB names (if not specified, auto-detect subdirectories under the <scenario>)
        List<String> dbNames =
                (dbNamesAttr != null && dbNamesAttr.length > 0) ? Arrays.asList(dbNamesAttr)
                        : detectDbNames(testClassRoot.resolve(scenarioName));

        for (String db : dbNames) {
            // Always obtain a Spring-managed DataSource
            Optional<DataSource> dsOpt = maybeGetSpringManagedDataSource(context, db);
            if (dsOpt.isEmpty()) {
                throw new IllegalStateException("No Spring-managed DataSource found for DB=" + db);
            }

            DataSource ds = dsOpt.get();
            Connection conn = DataSourceUtils.getConnection(ds);
            conn = wrapConnectionNoClose(conn);

            ConnectionConfig.Entry entry = buildEntryFromProps(db);
            loader.executeWithConnection(scenarioName, entry, conn);
        }
    }

    /**
     * Wrap a {@link Connection} that is participating in a Spring-managed transaction so that
     * {@code close()} calls are ignored.
     *
     * <p>
     * In a Spring Test {@code @Transactional} environment, the transaction manager must close the
     * connection at the end of the test. If external code such as DBUnit invokes {@code close()},
     * the connection could be forcibly closed, causing subsequent failures.
     * </p>
     *
     * <p>
     * The wrapped {@code Connection} ignores only {@code close()}, delegating all other methods to
     * the original.
     * </p>
     *
     * @param original the original JDBC connection (non-null)
     * @return a connection proxy that ignores {@code close()}
     */
    private Connection wrapConnectionNoClose(Connection original) {
        return (Connection) Proxy.newProxyInstance(original.getClass().getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    String name = method.getName();
                    if ("close".equals(name)) {
                        return null;
                    }
                    return method.invoke(original, args);
                });
    }

    /**
     * Returns a {@link DataSource} that is currently bound to Spring's test transaction, if any.
     *
     * <p>
     * Checks {@link TransactionSynchronizationManager#getResourceMap()} and returns the key that is
     * a {@link DataSource}.
     * </p>
     *
     * @param context JUnit {@link ExtensionContext}; not used
     * @param dbName logical database name; not used
     * @return {@link Optional} containing the bound {@link DataSource} if present; otherwise
     *         {@link Optional#empty()}
     */
    private Optional<DataSource> maybeGetSpringManagedDataSource(ExtensionContext context,
            String dbName) {

        // 1. Prefer a DataSource currently bound to the transaction
        Map<Object, Object> resourceMap = TransactionSynchronizationManager.getResourceMap();
        for (Map.Entry<Object, Object> e : resourceMap.entrySet()) {
            if (e.getKey() instanceof DataSource) {
                return Optional.of((DataSource) e.getKey());
            }
        }
        return Optional.empty();
    }

    /**
     * Build a {@link ConnectionConfig.Entry} from application.properties. Resolve
     * {@code url / username / password / driver-class-name} using suffix matching and contiguous
     * segment scoring.
     *
     * @param db DB logical name (nullable)
     * @return entry
     */
    private ConnectionConfig.Entry buildEntryFromProps(String db) {
        final String dbName = StringUtils.isBlank(db) ? null : db.trim();
        final String dbKeyLower = (dbName == null) ? null : dbName.toLowerCase(Locale.ROOT);

        final String url = resolvePropertyForDb(appProps, dbKeyLower, "url");
        final String user = resolvePropertyForDb(appProps, dbKeyLower, "username");
        final String pass = resolvePropertyForDb(appProps, dbKeyLower, "password");
        final String driver = resolvePropertyForDb(appProps, dbKeyLower, "driver-class-name");

        if (url == null || user == null) {
            throw new IllegalStateException("Missing connection info (Entry build, DB="
                    + (dbName == null ? "<none>" : dbName) + "): *[.]"
                    + (dbName == null ? "" : dbKeyLower + "[.]")
                    + "*.(url|username)[, password, driver-class-name]");
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
     * Flexibly resolve one property value matching the given DB name (optional) and suffix.
     *
     * <p>
     * This method searches through {@link Properties} keys and selects the one with the highest
     * score according to {@link #scoreKeyWithSegments(String, String[], String)}. Typical suffixes
     * are: {@code url}, {@code username}, {@code password}, {@code driver-class-name}.
     * </p>
     *
     * @param props all properties
     * @param dbNameLower lowercase DB name (e.g., {@code operator.bbb}); may be null
     * @param suffix one of {@code url}, {@code username}, {@code password},
     *        {@code driver-class-name}
     * @return the value, or {@code null} if none found
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

        if (bestKey != null) {
            return props.getProperty(bestKey);
        }
        return null;
    }

    /**
     * Score a property key based on priority (e.g., contiguous matches to DB name segments).
     *
     * <p>
     * Keys are scored higher when:
     * <ul>
     * <li>they contain contiguous matches of DB name segments,</li>
     * <li>matches appear near the key suffix,</li>
     * <li>they follow the typical Spring Boot convention ({@code spring.datasource.*}).</li>
     * </ul>
     * </p>
     *
     * @param keyLower lowercase property key (e.g., {@code spring.datasource.bbb.url})
     * @param dbSegs DB name segments (split by ".")
     * @param suffix suffix (e.g., {@code url}, {@code username})
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
     * Resolve the resources-root path corresponding to the test class (e.g.,
     * {@code com/example/MyTest}).
     *
     * @param testClass test class
     * @return root {@link Path}
     * @throws Exception if resolution fails
     */
    private Path resolveTestClassRoot(Class<?> testClass) throws Exception {
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        String folderName = testClass.getSimpleName();
        String resourcePath = pkgPath + "/" + folderName;
        URL url = Thread.currentThread().getContextClassLoader().getResource(resourcePath);
        if (url == null) {
            throw new IllegalStateException(
                    "Resource folder for the test class was not found: " + resourcePath);
        }
        return Paths.get(url.getPath());
    }

    /**
     * Resolve {@link LoadData#scenario()} to a list. If not specified, list subdirectories directly
     * under the class root.
     *
     * @param ann annotation
     * @return scenario names
     * @throws Exception if listing fails
     */
    private List<String> resolveScenarios(LoadData ann) throws Exception {
        if (ann.scenario().length > 0) {
            return Arrays.asList(ann.scenario());
        }
        return listDirectories(testClassRoot);
    }

    /**
     * List subdirectory names directly under the given parent.
     *
     * @param parent parent directory
     * @return subdirectory names
     * @throws Exception if listing fails
     */
    private List<String> listDirectories(Path parent) throws Exception {
        List<String> dirs = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(parent, Files::isDirectory)) {
            for (Path p : ds) {
                dirs.add(p.getFileName().toString());
            }
        }
        dirs.sort(Comparator.naturalOrder());
        return dirs;
    }

    /**
     * Detect DB names (directory names) directly under the scenario directory.
     *
     * @param scenarioDir scenario directory
     * @return DB names
     * @throws Exception if listing fails
     */
    private List<String> detectDbNames(Path scenarioDir) throws Exception {
        return listDirectories(scenarioDir);
    }

    /**
     * Load {@code application.properties} from the classpath and merge the active profile if
     * present.
     *
     * @return merged properties
     * @throws Exception if reading fails
     */
    private Properties loadAppProps() throws Exception {
        Properties props = new Properties();

        try (InputStream in = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("application.properties")) {
            if (in == null) {
                throw new IllegalStateException(
                        "application.properties was not found on the classpath");
            }
            props.load(in);
        }

        String profile = props.getProperty("spring.profiles.active");
        if (profile != null && !profile.isBlank()) {
            String profileFileName = String.format("application-%s.properties", profile);
            try (InputStream profileIn = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream(profileFileName)) {
                if (profileIn == null) {
                    throw new IllegalStateException(
                            profileFileName + " was not found on the classpath");
                }
                props.load(profileIn);
            }
        }

        return props;
    }
}
