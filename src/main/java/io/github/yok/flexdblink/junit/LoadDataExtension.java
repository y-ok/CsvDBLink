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
import io.github.yok.flexdblink.util.LogPathUtil;
import io.github.yok.flexdblink.util.OracleDateTimeFormatUtil;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * JUnit 5 Extension that interprets {@link LoadData} and loads CSV/LOB test data.
 *
 * <ul>
 * <li><b>TX policy:</b> Join an existing test transaction if present; otherwise start one and
 * always roll it back after the test.</li>
 * <li><b>Single-DB mode:</b> when {@code dbNames} is not specified. Uses {@code input/} directly
 * and conventional beans {@code transactionManager}/{@code dataSource}.</li>
 * <li><b>Multi-DB mode:</b> when {@code dbNames} is specified. Uses {@code input/{dbId}/}. DS is
 * identified by actual connection (URL/USER), and TM is a {@link DataSourceTransactionManager} that
 * holds the DS.</li>
 * <li><b>Directory validation:</b> missing dbId folders cause errors; extra ones are warned.</li>
 * <li>For Spring-managed connections, {@code close()} is disabled to prevent accidental
 * closing.</li>
 * </ul>
 *
 * <h3>Directory layout</h3>
 *
 * <pre>
 * Single DB: src/test/resources/{pkg}/{TestClass}/{scenario}/input/
 * Multi  DB: src/test/resources/{pkg}/{TestClass}/{scenario}/input/{dbId}/
 * </pre>
 */
@Slf4j
public class LoadDataExtension
        implements BeforeAllCallback, BeforeTestExecutionCallback, AfterTestExecutionCallback {

    // Shared context (classRoot / application*.properties, etc.)
    private TestResourceContext trc;

    private static final String EXCLUDE_FLYWAY_SCHEMA_HISTORY = "flyway_schema_history";

    // Store namespace and keys
    private static final Namespace NS = Namespace.create(LoadDataExtension.class.getName(), "TX");
    private static final String STORE_KEY_TX = "TX_RECORDS";
    private static final String STORE_KEY_TXI_DEFAULT = "TXI_DEFAULT_SWITCH";

    // Record container for TransactionInterceptor default TM switching
    private static final class TxInterceptorSwitchRecord {
        final Map<String, Boolean> touched = new LinkedHashMap<>();
    }

    /**
     * Initialize shared context at class start.
     *
     * @param context execution context.
     * @throws Exception when initialization fails.
     */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        this.trc = TestResourceContext.init(context);
        log.info("Initialization completed. classRoot={}, loadedProperties={}", trc.getClassRoot(),
                trc.getAppProps().size());
    }

    /**
     * Interpret {@link LoadData} and load data right before each test. Joins an existing
     * Spring-managed test transaction if available.
     *
     * @param context execution context.
     * @throws Exception on failure.
     */
    @Override
    public void beforeTestExecution(ExtensionContext context) throws Exception {
        if (trc == null) {
            beforeAll(context);
        }

        Class<?> testClass = context.getRequiredTestClass();
        log.info("Extension is active. testClass={}", testClass.getName());

        // Class-level @LoadData
        LoadData classAnn = testClass.getAnnotation(LoadData.class);
        if (classAnn != null) {
            for (String scenario : resolveScenarios(classAnn)) {
                Path base = StringUtils.isBlank(scenario) ? trc.getClassRoot()
                        : trc.getClassRoot().resolve(scenario);
                if (Files.isDirectory(base)) {
                    log.info("Found class-level @LoadData. scenario={}, dir={}", scenario, base);
                    loadScenarioParticipating(context, scenario, classAnn.dbNames());
                } else {
                    log.warn("Class-level scenario directory not found. scenario={}, dir={}",
                            scenario, base);
                }
            }
        }

        // Method-level @LoadData
        context.getTestMethod().ifPresent(m -> {
            LoadData methodAnn = m.getAnnotation(LoadData.class);
            if (methodAnn == null) {
                return;
            }
            try {
                for (String scenario : resolveScenarios(methodAnn)) {
                    Path base = StringUtils.isBlank(scenario) ? trc.getClassRoot()
                            : trc.getClassRoot().resolve(scenario);
                    if (Files.isDirectory(base)) {
                        log.info("Found method-level @LoadData. method={}, scenario={}, dir={}",
                                m.getName(), scenario, LogPathUtil.renderDirForLog(base.toFile()));
                        loadScenarioParticipating(context, scenario, methodAnn.dbNames());
                    } else {
                        log.info(
                                "Method-level scenario directory not found. method={}, scenario={}, dir={}",
                                m.getName(), scenario, LogPathUtil.renderDirForLog(base.toFile()));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Roll back test transactions started by this extension and restore AOP default TM.
     *
     * @param context execution context.
     */
    @Override
    public void afterTestExecution(ExtensionContext context) {
        rollbackAllIfBegan(context);
        restoreTxInterceptorDefaultManager(context);
    }

    /**
     * Load data according to the scenario/dbNames. Joins an existing transaction or starts a new
     * one (rolled back after the test).
     *
     * @param context execution context.
     * @param scenarioName scenario name (nullable/empty allowed).
     * @param dbNamesAttr dbIds specified by annotation (nullable/empty for single-DB mode).
     * @throws Exception on failure.
     */
    private void loadScenarioParticipating(ExtensionContext context, String scenarioName,
            String[] dbNamesAttr) throws Exception {

        // Prepare dump output directory
        Path dumpRoot = Paths.get(System.getProperty("user.dir"), "target", "dbunit", "dump")
                .toAbsolutePath().normalize();
        Files.createDirectories(dumpRoot);
        log.info("Dump directory prepared: {}", dumpRoot);

        // CSV/DateTime settings (Oracle compatible)
        CsvDateTimeFormatProperties dtProps = new CsvDateTimeFormatProperties();
        dtProps.setDate("yyyy-MM-dd");
        dtProps.setTime("HH:mm:ss");
        dtProps.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        dtProps.setDateTime("yyyy-MM-dd HH:mm:ss");

        DumpConfig dumpConfig = new DumpConfig();
        // Exclude Flyway history table
        List<String> merged = new ArrayList<>(
                Optional.ofNullable(dumpConfig.getExcludeTables()).orElse(List.of()));
        if (merged.stream().noneMatch(s -> EXCLUDE_FLYWAY_SCHEMA_HISTORY.equalsIgnoreCase(s))) {
            merged.add(EXCLUDE_FLYWAY_SCHEMA_HISTORY);
        }
        dumpConfig.setExcludeTables(ImmutableList.copyOf(merged));
        log.info("Excluded tables: {}", dumpConfig.getExcludeTables());

        DbUnitConfigFactory configFactory = new DbUnitConfigFactory();
        ConnectionConfig connectionConfig = new ConnectionConfig();
        OracleDateTimeFormatUtil dateTimeUtil = new OracleDateTimeFormatUtil(dtProps);

        // Resolve paths
        Path classRoot = trc.getClassRoot();
        PathsConfig pathsConfig = new PathsConfig() {
            @Override
            public String getLoad() {
                return classRoot.toAbsolutePath().toString();
            }

            @Override
            public String getDataPath() {
                return classRoot.toAbsolutePath().toString();
            }

            @Override
            public String getDump() {
                return dumpRoot.toString();
            }
        };

        DbUnitConfig dbUnitConfig = new DbUnitConfig();
        DbDialectHandlerFactory handlerFactory = new DbDialectHandlerFactory(dbUnitConfig,
                dumpConfig, pathsConfig, dateTimeUtil, configFactory);

        DataLoader loader = new DataLoader(pathsConfig, connectionConfig,
                entry -> Optional.ofNullable(entry.getUser()).map(u -> u.toUpperCase(Locale.ROOT))
                        .orElseThrow(() -> new IllegalStateException(
                                "Failed to resolve schema: user is undefined")),
                handlerFactory::create, dbUnitConfig, configFactory, dumpConfig);

        // Mode detection
        boolean multi = dbNamesAttr != null && dbNamesAttr.length > 0;
        ApplicationContext ac = getApplicationContext(context);

        // ===== Single-DB mode =====
        if (!multi) {
            PlatformTransactionManager tm = resolveTxManagerSingle(ac);
            DataSource ds = resolveDataSourceSingle(ac);

            setTxInterceptorDefaultManager(context, ac, tm, "SINGLE");

            // Start a test TX if none exists (always rolled back after the test)
            if (!isTxActive()) {
                beginTestTx("SINGLE", tm, context);
            } else if (!TransactionSynchronizationManager.hasResource(ds)) {
                log.warn(
                        "Single-DB mode: Existing transaction detected but the DataSource is not bound. Changes may persist after the test.");
            }

            // Dataset: input/
            Path datasetDir = trc.baseInputDir(scenarioName);
            if (!Files.isDirectory(datasetDir)) {
                throw new IllegalStateException(
                        "Dataset directory not found: " + datasetDir.toAbsolutePath());
            }

            // Connection info (single DB)
            ConnectionConfig.Entry entry = trc.buildEntryFromProps("");
            log.info("Connection info resolved. url={}, user={}", entry.getUrl(), entry.getUser());

            // Obtain connection (joins the TX automatically). Disable close().
            Connection conn = DataSourceUtils.getConnection(ds);
            conn = wrapConnectionNoClose(conn);
            log.info("Transactional connection obtained (single-DB mode).");

            // Execute
            loader.executeWithConnection(datasetDir.toFile(), entry, conn);
            log.info("Data load completed (single-DB mode). dir={}", datasetDir);
            return;
        }

        // ===== Multi-DB mode =====
        Set<String> expected = new LinkedHashSet<>(Arrays.asList(dbNamesAttr));
        Path baseInput = trc.baseInputDir(scenarioName);
        Set<String> found = listDbIdsFromFolder(baseInput);

        // Missing = error; Extras = warn
        Set<String> missing = new LinkedHashSet<>(expected);
        missing.removeAll(found);
        Set<String> extras = new LinkedHashSet<>(found);
        extras.removeAll(expected);
        if (!missing.isEmpty()) {
            throw new IllegalStateException(
                    "Folders corresponding to the specified dbNames were not found. missing="
                            + missing + ", base=" + baseInput);
        }
        if (!extras.isEmpty()) {
            log.warn(
                    "Extra folders under input/ not listed in dbNames were found (ignored). extras={}",
                    extras);
        }

        // Pre-resolve TM/DS set for each dbId
        List<String> dbIds = new ArrayList<>(expected);
        List<DataSource> dss = new ArrayList<>(dbIds.size());
        List<PlatformTransactionManager> tms = new ArrayList<>(dbIds.size());

        for (String dbId : dbIds) {
            Components comp = resolveComponentsForDbId(ac, dbId);
            dss.add(comp.dataSource);
            tms.add(comp.txManager);
        }

        if (dbIds.size() == 1) {
            // Only one: switch AOP default TM explicitly
            setTxInterceptorDefaultManager(context, ac, tms.get(0), "MULTI:" + dbIds.get(0));
        } else {
            // 2 or more: do not switch; require explicit @Transactional if needed
            log.warn(
                    "Multi-DB mode detected (dbIds={}). Skipping AOP default TM switching. Specify @Transactional(transactionManager=\"...\") explicitly if necessary.",
                    dbIds);
        }

        // Check existing TX
        boolean active = isTxActive();
        if (active) {
            // With an existing TX, require all DS to be bound
            for (int i = 0; i < dss.size(); i++) {
                DataSource ds = dss.get(i);
                String dbId = dbIds.get(i);
                if (!TransactionSynchronizationManager.hasResource(ds)) {
                    throw new IllegalStateException(
                            "Detected an existing test transaction, but the DataSource is not participating. dbId="
                                    + dbId);
                }
            }
            log.info("All DBs are participating in the existing transaction. Running within it.");
        } else {
            // No existing TX: start per dbId (rolled back afterwards)
            for (int i = 0; i < dbIds.size(); i++) {
                beginTestTx(dbIds.get(i), tms.get(i), context);
            }
        }

        // Execute per dbId
        for (int i = 0; i < dbIds.size(); i++) {
            String dbId = dbIds.get(i);
            DataSource ds = dss.get(i);

            // Dataset: input/{dbId}/
            Path datasetDir = trc.inputDir(scenarioName, dbId);
            if (!Files.isDirectory(datasetDir)) {
                throw new IllegalStateException(
                        "Dataset directory not found: " + datasetDir.toAbsolutePath());
            }

            // Connection info
            ConnectionConfig.Entry entry = trc.buildEntryFromProps(dbId);
            log.info("Connection info resolved. dbId={}, url={}, user={}", entry.getId(),
                    entry.getUrl(), entry.getUser());

            // Obtain connection (joins the TX automatically). Disable close().
            Connection conn = DataSourceUtils.getConnection(ds);
            conn = wrapConnectionNoClose(conn);
            log.info("Transactional connection obtained. dbId={}", dbId);

            // Execute
            loader.executeWithConnection(datasetDir.toFile(), entry, conn);
            log.info("Data load completed. dbId={}, dir={}", dbId,
                    LogPathUtil.renderDirForLog(datasetDir.toFile()));
        }
    }

    /**
     * Switch the default TransactionManager of every {@link TransactionInterceptor} in the context
     * to the selected TM's bean name. Restored after the test.
     */
    private void setTxInterceptorDefaultManager(ExtensionContext context, ApplicationContext ac,
            PlatformTransactionManager picked, String key) {

        String pickedName = findBeanNameByInstance(ac, PlatformTransactionManager.class, picked);
        if ("<unknown>".equals(pickedName)) {
            log.warn(
                    "AOP coordination: Could not resolve bean name of the selected TransactionManager. Skipping switch. key={}",
                    key);
            return;
        }

        Map<String, TransactionInterceptor> tis = ac.getBeansOfType(TransactionInterceptor.class);
        if (tis.isEmpty()) {
            log.info(
                    "AOP coordination: No TransactionInterceptor beans found. Switching is unnecessary.");
            return;
        }

        Store store = context.getStore(NS);
        TxInterceptorSwitchRecord rec = store.getOrComputeIfAbsent(STORE_KEY_TXI_DEFAULT,
                k -> new TxInterceptorSwitchRecord(), TxInterceptorSwitchRecord.class);

        for (Map.Entry<String, TransactionInterceptor> e : tis.entrySet()) {
            String tiName = e.getKey();
            TransactionInterceptor ti = e.getValue();
            try {
                // Clear explicit TM instance and switch to bean-name resolution
                ti.setTransactionManager(null);
                ti.setTransactionManagerBeanName(pickedName);
                rec.touched.put(tiName, Boolean.TRUE);
            } catch (Exception ex) {
                log.warn(
                        "AOP coordination: Failed to switch default TransactionManager. interceptor={}, key={}",
                        tiName, key, ex);
            }
        }
        log.info(
                "AOP coordination: Default TransactionManager switched. picked={}, key={}, affectedInterceptors={}",
                pickedName, key, tis.size());
    }

    /**
     * Restore the default TransactionManager settings for {@link TransactionInterceptor}s that were
     * modified by {@link #setTxInterceptorDefaultManager}.
     */
    private void restoreTxInterceptorDefaultManager(ExtensionContext context) {
        Store store = context.getStore(NS);
        TxInterceptorSwitchRecord rec =
                store.get(STORE_KEY_TXI_DEFAULT, TxInterceptorSwitchRecord.class);
        if (rec == null || rec.touched.isEmpty()) {
            return;
        }

        ApplicationContext ac = getApplicationContext(context);
        Map<String, TransactionInterceptor> tis = ac.getBeansOfType(TransactionInterceptor.class);
        for (Map.Entry<String, Boolean> e : rec.touched.entrySet()) {
            String tiName = e.getKey();
            TransactionInterceptor ti = tis.get(tiName);
            if (ti == null) {
                continue;
            }
            try {
                ti.setTransactionManagerBeanName(null);
                ti.setTransactionManager(null);
            } catch (Exception ex) {
                log.warn(
                        "AOP coordination: Failed to restore default TransactionManager. interceptor={}",
                        tiName, ex);
            }
        }
        store.remove(STORE_KEY_TXI_DEFAULT);
        log.info("AOP coordination: Default TransactionManager restored. affectedInterceptors={}",
                rec.touched.size());
    }

    /**
     * Resolve {@link LoadData#scenario()}; if not specified, list directories under the class root.
     *
     * @param ann annotation.
     * @return scenario names.
     * @throws Exception on failure.
     */
    private List<String> resolveScenarios(LoadData ann) throws Exception {
        if (ann.scenario().length > 0) {
            return Arrays.asList(ann.scenario());
        }
        return trc.listDirectories(trc.getClassRoot());
    }

    /**
     * Wrap a connection to ignore {@code close()} while participating in a Spring-managed TX.
     *
     * @param original original connection.
     * @return proxy connection that ignores {@code close()}.
     */
    private Connection wrapConnectionNoClose(Connection original) {
        return (Connection) Proxy.newProxyInstance(original.getClass().getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    return method.invoke(original, args);
                });
    }

    /**
     * Obtain {@link ApplicationContext}.
     *
     * @param context execution context.
     * @return application context.
     */
    private ApplicationContext getApplicationContext(ExtensionContext context) {
        return SpringExtension.getApplicationContext(context);
    }

    /**
     * Whether a Spring-managed transaction is active on the current thread.
     *
     * @return {@code true} if active.
     */
    private boolean isTxActive() {
        return TransactionSynchronizationManager.isActualTransactionActive();
    }

    /**
     * Begin a test transaction and record it so it can be rolled back in the After phase.
     *
     * @param key identifier for logging (dbId, etc.).
     * @param tm transaction manager.
     * @param context execution context.
     */
    private void beginTestTx(String key, PlatformTransactionManager tm, ExtensionContext context) {
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        TransactionStatus status = tm.getTransaction(def);
        List<TxRecord> records = getOrCreateTxRecords(context);
        records.add(new TxRecord(key, tm, status));
        log.info("Started a test transaction. key={}", key);
    }

    /**
     * Roll back all test transactions started by this extension in reverse order.
     *
     * @param context execution context.
     */
    private void rollbackAllIfBegan(ExtensionContext context) {
        List<TxRecord> records = getTxRecords(context);
        if (records.isEmpty()) {
            return;
        }
        List<TxRecord> rev = com.google.common.collect.Lists.reverse(records);
        for (TxRecord r : rev) {
            try {
                r.tm.rollback(r.status);
                log.info("Rolled back due to test completion. key={}", r.key);
            } catch (Exception e) {
                log.error("Exception occurred during rollback. key={}", r.key, e);
            }
        }
        context.getStore(NS).remove(STORE_KEY_TX);
    }

    /**
     * Resolve the conventional TM bean ('transactionManager') for single-DB mode.
     *
     * @param ac application context.
     * @return PlatformTransactionManager.
     */
    private PlatformTransactionManager resolveTxManagerSingle(ApplicationContext ac) {
        try {
            return ac.getBean("transactionManager", PlatformTransactionManager.class);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Transaction manager 'transactionManager' not found. It is required in single-DB mode.",
                    e);
        }
    }

    /**
     * Resolve the conventional DS bean ('dataSource') for single-DB mode.
     *
     * @param ac application context.
     * @return DataSource.
     */
    private DataSource resolveDataSourceSingle(ApplicationContext ac) {
        try {
            return ac.getBean("dataSource", DataSource.class);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "DataSource 'dataSource' not found. It is required in single-DB mode.", e);
        }
    }

    /**
     * Resolve DS/TM pair for a given dbId based on actual connection metadata and types.
     *
     * @param ac application context.
     * @param dbId DB identifier.
     * @return resolved components.
     */
    private Components resolveComponentsForDbId(ApplicationContext ac, String dbId) {
        ConnectionConfig.Entry entry = trc.buildEntryFromProps(dbId);
        String expectedUrl = entry.getUrl();
        String expectedUser = entry.getUser();

        // Try TM side first
        List<TmWithDs> tmMatches = findTmByMetadata(ac, expectedUrl, expectedUser);
        if (tmMatches.size() == 1) {
            TmWithDs m = tmMatches.get(0);
            log.info("Uniquely identified by TM. dbId={}, tm={}, ds={}", dbId, m.tmName, m.dsName);
            return new Components(m.ds, m.tm);
        }
        if (tmMatches.size() > 1) {
            List<String> names = new ArrayList<>();
            for (TmWithDs m : tmMatches) {
                names.add(m.tmName);
            }
            throw new IllegalStateException(
                    "Multiple matching TransactionManagers were found for dbId=" + dbId
                            + ". candidates=" + names);
        }

        // Decide from DS side
        List<NamedDs> dsMatches = findDataSourceByMetadata(ac, expectedUrl, expectedUser);
        if (dsMatches.isEmpty()) {
            String[] all = ac.getBeanNamesForType(DataSource.class);
            throw new IllegalStateException("No matching DataSource was found for dbId=" + dbId
                    + ". expectedUrl=" + expectedUrl + ", expectedUser=" + expectedUser
                    + " candidates=" + Arrays.toString(all));
        }
        if (dsMatches.size() == 1) {
            NamedDs only = dsMatches.get(0);
            PlatformTransactionManager tm = resolveTxManagerByDataSource(ac, dbId, only.ds);
            return new Components(only.ds, tm);
        }

        // Prefer single @Primary DS
        NamedDs primary = pickPrimary(ac, dsMatches);
        if (primary != null) {
            PlatformTransactionManager tm = resolveTxManagerByDataSource(ac, dbId, primary.ds);
            log.info("Adopted @Primary DataSource. dbId={}, ds={}", dbId, primary.name);
            return new Components(primary.ds, tm);
        }

        // If exactly one DS is referenced by TMs, adopt it
        NamedDs referencedSingle = pickSingleReferencedByTm(ac, dsMatches);
        if (referencedSingle != null) {
            PlatformTransactionManager tm =
                    resolveTxManagerByDataSource(ac, dbId, referencedSingle.ds);
            log.info("Adopted the only DS referenced by a TM. dbId={}, ds={}", dbId,
                    referencedSingle.name);
            return new Components(referencedSingle.ds, tm);
        }

        // Still ambiguous
        List<String> names = new ArrayList<>();
        for (NamedDs n : dsMatches) {
            names.add(n.name);
        }
        throw new IllegalStateException("Multiple matching DataSources were found for dbId=" + dbId
                + ". candidates=" + names);
    }

    /**
     * Find TMs whose DS metadata matches URL/USER.
     *
     * @param ac application context.
     * @param expectedUrl expected URL.
     * @param expectedUser expected USER.
     * @return matching TMs.
     */
    private List<TmWithDs> findTmByMetadata(ApplicationContext ac, String expectedUrl,
            String expectedUser) {
        List<TmWithDs> result = new ArrayList<>();
        String[] tmNames = ac.getBeanNamesForType(PlatformTransactionManager.class);
        for (String tmName : tmNames) {
            PlatformTransactionManager tm = ac.getBean(tmName, PlatformTransactionManager.class);
            if (tm instanceof DataSourceTransactionManager) {
                DataSource ds = ((DataSourceTransactionManager) tm).getDataSource();
                if (ds == null) {
                    continue;
                }
                ProbeMeta meta = probeDataSourceMeta(ds);
                if (meta == null) {
                    continue;
                }
                if (urlRoughMatch(expectedUrl, meta.url)
                        && equalsIgnoreCaseSafe(expectedUser, meta.user)) {
                    String dsName = findBeanNameByInstance(ac, DataSource.class, ds);
                    result.add(new TmWithDs(tmName, dsName, tm, ds));
                }
            }
        }
        return result;
    }

    /**
     * Find DS that match URL/USER.
     *
     * @param ac application context.
     * @param expectedUrl expected URL.
     * @param expectedUser expected USER.
     * @return matching DS list.
     */
    private List<NamedDs> findDataSourceByMetadata(ApplicationContext ac, String expectedUrl,
            String expectedUser) {
        List<NamedDs> result = new ArrayList<>();
        String[] dsNames = ac.getBeanNamesForType(DataSource.class);
        for (String name : dsNames) {
            DataSource ds = ac.getBean(name, DataSource.class);
            ProbeMeta meta = probeDataSourceMeta(ds);
            if (meta == null) {
                continue;
            }
            if (urlRoughMatch(expectedUrl, meta.url)
                    && equalsIgnoreCaseSafe(expectedUser, meta.user)) {
                result.add(new NamedDs(name, ds));
            }
        }
        return result;
    }

    /**
     * Return single {@code @Primary} DS among candidates, if present.
     *
     * @param ac application context.
     * @param candidates DS candidates.
     * @return single primary DS or {@code null}.
     */
    private NamedDs pickPrimary(ApplicationContext ac, List<NamedDs> candidates) {
        if (ac instanceof ConfigurableApplicationContext) {
            ConfigurableListableBeanFactory bf =
                    ((ConfigurableApplicationContext) ac).getBeanFactory();
            NamedDs found = null;
            for (NamedDs n : candidates) {
                if (bf.containsBeanDefinition(n.name)) {
                    BeanDefinition bd = bf.getBeanDefinition(n.name);
                    if (bd.isPrimary()) {
                        if (found != null) {
                            // Multiple primaries → treat as ambiguous
                            return null;
                        }
                        found = n;
                    }
                }
            }
            return found;
        }
        return null;
    }

    /**
     * Among DS candidates, return the only DS referenced by any TM.
     *
     * @param ac application context.
     * @param candidates DS candidates.
     * @return single referenced DS or {@code null}.
     */
    private NamedDs pickSingleReferencedByTm(ApplicationContext ac, List<NamedDs> candidates) {
        String[] tmNames = ac.getBeanNamesForType(PlatformTransactionManager.class);
        NamedDs only = null;
        for (NamedDs n : candidates) {
            boolean referenced = false;
            for (String tmName : tmNames) {
                PlatformTransactionManager tm =
                        ac.getBean(tmName, PlatformTransactionManager.class);
                if (tm instanceof DataSourceTransactionManager) {
                    DataSource tmDs = ((DataSourceTransactionManager) tm).getDataSource();
                    if (tmDs == n.ds) {
                        referenced = true;
                        break;
                    }
                }
            }
            if (referenced) {
                if (only != null) {
                    // More than one referenced → ambiguous
                    return null;
                }
                only = n;
            }
        }
        return only;
    }

    /**
     * Identify the single {@link DataSourceTransactionManager} that manages the given DS (by
     * reference equality).
     *
     * @param ac application context.
     * @param dbId DB identifier (for logs).
     * @param ds target DS.
     * @return PlatformTransactionManager.
     */
    private PlatformTransactionManager resolveTxManagerByDataSource(ApplicationContext ac,
            String dbId, DataSource ds) {
        String[] tmNames = ac.getBeanNamesForType(PlatformTransactionManager.class);
        List<String> hits = new ArrayList<>();
        PlatformTransactionManager matched = null;

        for (String name : tmNames) {
            PlatformTransactionManager tm = ac.getBean(name, PlatformTransactionManager.class);
            if (tm instanceof DataSourceTransactionManager) {
                DataSource tmDs = ((DataSourceTransactionManager) tm).getDataSource();
                if (tmDs == ds) {
                    hits.add(name);
                    matched = tm;
                }
            }
        }

        if (hits.isEmpty()) {
            throw new IllegalStateException(
                    "No DataSourceTransactionManager managing the target DataSource was found for dbId="
                            + dbId
                            + ". Define a TransactionManager bound to the target DataSource. candidates="
                            + Arrays.toString(tmNames));
        }
        if (hits.size() > 1) {
            throw new IllegalStateException(
                    "Multiple TransactionManagers managing the target DataSource were found for dbId="
                            + dbId + ". candidates=" + hits);
        }
        return matched;
    }

    /**
     * List subdirectory names (= DB IDs) directly under {@code input/}.
     *
     * @param baseInputDir scenario input directory.
     * @return set of subdirectory names.
     * @throws Exception on failure.
     */
    private Set<String> listDbIdsFromFolder(Path baseInputDir) throws Exception {
        if (!Files.isDirectory(baseInputDir)) {
            return Collections.emptySet();
        }
        try {
            Set<String> result = new LinkedHashSet<>();
            try (var stream = Files.list(baseInputDir)) {
                stream.filter(Files::isDirectory).map(p -> p.getFileName().toString())
                        .forEach(result::add);
            }
            return result;
        } catch (Exception e) {
            throw new Exception("Failed to scan DB ID folders under 'input/': " + baseInputDir, e);
        }
    }

    /**
     * Get connection metadata (URL/USER) from a DataSource. Returns {@code null} on failure. Uses a
     * direct {@code getConnection()} and immediate close to avoid joining a TX.
     *
     * @param ds DataSource.
     * @return metadata or {@code null}.
     */
    private ProbeMeta probeDataSourceMeta(DataSource ds) {
        try (Connection c = ds.getConnection()) {
            String url = (c.getMetaData() != null) ? c.getMetaData().getURL() : null;
            String user = (c.getMetaData() != null) ? c.getMetaData().getUserName() : null;
            return new ProbeMeta(url, user);
        } catch (Exception e) {
            log.debug("Failed to obtain DataSource metadata.", e);
            return null;
        }
    }

    /**
     * Simplify URL and check partial containment (strip query/semicolon and lowercase).
     *
     * @param expected expected URL.
     * @param actual actual URL.
     * @return whether they roughly match.
     */
    private boolean urlRoughMatch(String expected, String actual) {
        String e = simplifyUrl(expected);
        String a = simplifyUrl(actual);
        if (e.isEmpty() || a.isEmpty()) {
            return false;
        }
        return a.contains(e) || e.contains(a);
    }

    /**
     * Strip query/semicolon and lowercase.
     *
     * @param url URL.
     * @return normalized URL.
     */
    private String simplifyUrl(String url) {
        if (url == null) {
            return "";
        }
        String s = url;
        int q = s.indexOf('?');
        if (q >= 0) {
            s = s.substring(0, q);
        }
        int sc = s.indexOf(';');
        if (sc >= 0) {
            s = s.substring(0, sc);
        }
        return s.toLowerCase(Locale.ROOT).trim();
    }

    /**
     * Null-safe, case-insensitive equality.
     *
     * @param a string A.
     * @param b string B.
     * @return equality result.
     */
    private boolean equalsIgnoreCaseSafe(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        return a.equalsIgnoreCase(b);
    }

    /**
     * Resolve bean name by instance (reference equality).
     *
     * @param ac application context.
     * @param type bean type.
     * @param instance bean instance.
     * @param <T> type parameter.
     * @return bean name or {@code "<unknown>"}.
     */
    private <T> String findBeanNameByInstance(ApplicationContext ac, Class<T> type, T instance) {
        String[] names = ac.getBeanNamesForType(type);
        for (String name : names) {
            T bean = ac.getBean(name, type);
            if (bean == instance) {
                return name;
            }
        }
        return "<unknown>";
    }

    /**
     * Record for a test transaction started by this extension.
     */
    private static final class TxRecord {
        final String key;
        final PlatformTransactionManager tm;
        final TransactionStatus status;

        TxRecord(String key, PlatformTransactionManager tm, TransactionStatus status) {
            this.key = key;
            this.tm = tm;
            this.status = status;
        }
    }

    /**
     * Pair of DS/TM.
     */
    private static final class Components {
        final DataSource dataSource;
        final PlatformTransactionManager txManager;

        Components(DataSource dataSource, PlatformTransactionManager txManager) {
            this.dataSource = dataSource;
            this.txManager = txManager;
        }
    }

    /**
     * Pair of TM and DS names/instances.
     */
    private static final class TmWithDs {
        final String tmName;
        final String dsName;
        final PlatformTransactionManager tm;
        final DataSource ds;

        TmWithDs(String tmName, String dsName, PlatformTransactionManager tm, DataSource ds) {
            this.tmName = tmName;
            this.dsName = dsName;
            this.tm = tm;
            this.ds = ds;
        }
    }

    /**
     * Named DataSource wrapper.
     */
    private static final class NamedDs {
        final String name;
        final DataSource ds;

        NamedDs(String name, DataSource ds) {
            this.name = name;
            this.ds = ds;
        }
    }

    /**
     * Get or create transaction records list.
     *
     * @param context execution context.
     * @return records list.
     */
    private List<TxRecord> getOrCreateTxRecords(ExtensionContext context) {
        Store store = context.getStore(NS);
        TxRecordList list = store.getOrComputeIfAbsent(STORE_KEY_TX, k -> new TxRecordList(),
                TxRecordList.class);
        return list;
    }

    /**
     * Get transaction records list started by this extension (empty if none).
     *
     * @param context execution context.
     * @return records list.
     */
    private List<TxRecord> getTxRecords(ExtensionContext context) {
        Store store = context.getStore(NS);
        TxRecordList list = store.get(STORE_KEY_TX, TxRecordList.class);
        return (list != null) ? list : new TxRecordList();
    }

    /**
     * Type-safe list container for TxRecord.
     */
    private static final class TxRecordList extends ArrayList<TxRecord> {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Internal container for DS connection metadata (URL and USER).
     */
    private static final class ProbeMeta {
        final String url;
        final String user;

        ProbeMeta(String url, String user) {
            this.url = url;
            this.user = user;
        }
    }
}
