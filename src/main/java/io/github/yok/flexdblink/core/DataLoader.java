package io.github.yok.flexdblink.core;

import com.google.common.collect.Lists;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.db.LobResolvingTableWrapper;
import io.github.yok.flexdblink.parser.CsvDataParser;
import io.github.yok.flexdblink.parser.DataFormat;
import io.github.yok.flexdblink.parser.DataLoaderFactory;
import io.github.yok.flexdblink.parser.DataParser;
import io.github.yok.flexdblink.util.ErrorHandler;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.filter.DefaultColumnFilter;
import org.dbunit.operation.DatabaseOperation;

/**
 * Utility class that loads CSV files and external LOB files via DBUnit and performs data loads
 * (CLEAN_INSERT / UPDATE / INSERT) into Oracle and other RDBMS products.
 *
 * <p>
 * <strong>Operating modes:</strong>
 * </p>
 * <ul>
 * <li><strong>initial mode</strong>
 * <ul>
 * <li>Insert <em>non-LOB</em> columns using {@link DatabaseOperation#CLEAN_INSERT}.</li>
 * <li>Then apply all columns including LOBs using {@link DatabaseOperation#UPDATE}.</li>
 * </ul>
 * </li>
 * <li><strong>scenario mode</strong>
 * <ul>
 * <li>Delete rows from DB that are exact duplicates of those already inserted in
 * <em>initial</em>.</li>
 * <li>Execute {@link DatabaseOperation#INSERT} only for rows that do not exist in initial
 * data.</li>
 * </ul>
 * </li>
 * </ul>
 *
 * <p>
 * This class does not alter business logic; only configuration-driven behavior and logging.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
@RequiredArgsConstructor
public class DataLoader {

    // Base directory settings for CSV/LOB files
    private final PathsConfig pathsConfig;

    // Holder of JDBC connection settings
    private final ConnectionConfig connectionConfig;

    // Function to resolve schema name from a ConnectionConfig.Entry
    private final Function<ConnectionConfig.Entry, String> schemaNameResolver;

    // Factory function to create a DB dialect handler
    private final Function<ConnectionConfig.Entry, DbDialectHandler> dialectFactory;

    // Configuration class holding dbunit.* settings from application.yml
    private final DbUnitConfig dbUnitConfig;

    // Factory to apply common DBUnit settings
    private final DbUnitConfigFactory configFactory;

    // Exclude-table settings for dump/load (dump.exclude-tables)
    private final DumpConfig dumpConfig;

    // Insert summary: dbId → (table → total inserted count)
    private final Map<String, Map<String, Integer>> insertSummary = new LinkedHashMap<>();

    /**
     * Entry point for data loading.
     *
     * @param scenario scenario name; if {@code null} or empty, only {@link DbUnitConfig#preDirName}
     *        is executed
     * @param targetDbIds list of target DB IDs; if {@code null} or empty, all DBs are targeted
     */
    public void execute(String scenario, List<String> targetDbIds) {
        String preMode = dbUnitConfig.getPreDirName();
        String mode = (scenario == null || scenario.isEmpty()) ? preMode : scenario;
        log.info("=== DataLoader started (mode={}, target DBs={}) ===", mode, targetDbIds);

        for (ConnectionConfig.Entry entry : connectionConfig.getConnections()) {
            String dbId = entry.getId();
            if (targetDbIds != null && !targetDbIds.isEmpty() && !targetDbIds.contains(dbId)) {
                log.info("[{}] Not targeted → skipping", dbId);
                continue;
            }

            DbDialectHandler dialectHandler = dialectFactory.apply(entry);

            // initial mode load
            File initialDir = new File(pathsConfig.getLoad(), preMode + File.separator + dbId);
            deploy(initialDir, dbId, true, entry, dialectHandler,
                    "Initial data load failed (DB=" + dbId + ")");

            // scenario mode load
            if (!preMode.equals(mode)) {
                File scenarioDir = new File(pathsConfig.getLoad(), mode + File.separator + dbId);
                deploy(scenarioDir, dbId, false, entry, dialectHandler,
                        "Scenario data load failed (DB=" + dbId + ")");
            }
        }

        log.info("=== DataLoader finished ===");
        logSummary();
    }

    /**
     * Reads CSV/LOB from the specified directory. In <em>initial</em> mode, performs CLEAN_INSERT +
     * UPDATE. In <em>scenario</em> mode, deletes rows that are duplicates of initial and INSERTs
     * the remainder.
     *
     * @param dir directory where CSV/LOB are located
     * @param dbId connections.id (for logging)
     * @param initial {@code true}=initial mode, {@code false}=scenario mode
     * @param entry JDBC connection info
     * @param dialectHandler DB dialect handler providing vendor-specific behavior
     * @param errorMessage log message to output on fatal error
     */
    private void deploy(File dir, String dbId, boolean initial, ConnectionConfig.Entry entry,
            DbDialectHandler dialectHandler, String errorMessage) {
        if (!dir.exists()) {
            log.warn("[{}] Directory does not exist → skipping", dbId);
            return;
        }

        try {
            ensureTableOrdering(dir);

            DataParser parser = new CsvDataParser();
            IDataSet dataSet;
            try {
                dataSet = parser.parse(dir);
            } catch (DataSetException e) {
                log.warn("[{}] Failed to create CsvDataSet → skipping: {}", dbId, e.getMessage());
                return;
            }

            List<String> tables = Lists.newArrayList(dataSet.getTableNames());
            if (tables.isEmpty()) {
                log.info("[{}] No tables → skipping", dbId);
                return;
            }
            tables.sort(String::compareTo);

            if (dumpConfig != null && dumpConfig.getExcludeTables() != null
                    && !dumpConfig.getExcludeTables().isEmpty()) {
                // Build a lowercase set for case-insensitive compares
                final Set<String> excludeLower =
                        dumpConfig.getExcludeTables().stream().filter(Objects::nonNull)
                                .map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());

                // Log excluded targets
                log.info("[{}] Excluded tables: {}", dbId, excludeLower);

                // Remove excluded tables
                tables = tables.stream()
                        .filter(t -> !excludeLower.contains(t.toLowerCase(Locale.ROOT)))
                        .collect(Collectors.toList());

                if (tables.isEmpty()) {
                    log.info("[{}] No effective tables (all excluded) → skipping", dbId);
                    return;
                }
            }

            Class.forName(entry.getDriverClass());
            try (Connection jdbc = DriverManager.getConnection(entry.getUrl(), entry.getUser(),
                    entry.getPassword())) {

                DatabaseConnection dbConn = createDbUnitConn(jdbc, entry, dialectHandler);
                String schema = schemaNameResolver.apply(entry);

                for (String table : tables) {
                    dialectHandler.logTableDefinition(jdbc, schema, table, dbId);

                    ITable base = dataSet.getTable(table);
                    int csvCount = base.getRowCount();
                    log.info("[{}] Table[{}] CSV rows={}", dbId, table, csvCount);

                    ITable wrapped = new LobResolvingTableWrapper(base, dir, dialectHandler);
                    DefaultDataSet ds = new DefaultDataSet(wrapped);

                    int inserted;
                    if (initial) {
                        // initial mode (CLEAN_INSERT + UPDATE)
                        Column[] lobCols = dialectHandler.getLobColumns(dir.toPath(), table);
                        if (lobCols.length > 0) {
                            boolean anyNotNullLob = dialectHandler.hasNotNullLobColumn(jdbc, schema,
                                    table, lobCols);
                            if (anyNotNullLob) {
                                log.info(
                                        "[{}] Table[{}] NOT NULL LOB present → CLEAN_INSERT all columns",
                                        dbId, table);
                                DatabaseOperation.CLEAN_INSERT.execute(dbConn, ds);
                            } else {
                                DatabaseOperation.CLEAN_INSERT.execute(dbConn, new DefaultDataSet(
                                        DefaultColumnFilter.excludedColumnsTable(base, lobCols)));
                                DatabaseOperation.UPDATE.execute(dbConn, ds);
                            }
                        } else {
                            DatabaseOperation.CLEAN_INSERT.execute(dbConn, ds);
                        }
                        inserted = csvCount;
                        log.info("[{}] Table[{}] Initial | inserted={}", dbId, table, inserted);

                    } else {
                        // scenario mode (delete duplicates + INSERT new)
                        // --- 1) Snapshot of DB at pre-time ---
                        ITable originalDbTable = dbConn.createDataSet().getTable(table);

                        // --- 2) Query primary keys ---
                        List<String> pkCols =
                                dialectHandler.getPrimaryKeyColumns(jdbc, schema, table);

                        // --- 3) Detect mapping of duplicate rows (CSV row → DB row) ---
                        Map<Integer, Integer> identicalMap = new LinkedHashMap<>();
                        Column[] cols = wrapped.getTableMetaData().getColumns();

                        if (!pkCols.isEmpty()) {
                            // With PK: match by keys
                            for (int i = 0; i < wrapped.getRowCount(); i++) {
                                for (int j = 0; j < originalDbTable.getRowCount(); j++) {
                                    boolean match = true;
                                    for (String pk : pkCols) {
                                        Object v1 = wrapped.getValue(i, pk);
                                        Object v2 = originalDbTable.getValue(j, pk);
                                        if (v1 == null ? v2 != null : !v1.equals(v2)) {
                                            match = false;
                                            break;
                                        }
                                    }
                                    if (match) {
                                        identicalMap.put(i, j);
                                        log.debug(
                                                "[{}] Table[{}] Duplicate detected: csvRow={} matches dbRow={}",
                                                dbId, table, i, j);
                                        break;
                                    }
                                }
                            }
                        } else {
                            // No PK: determine equality by comparing all columns
                            for (int i = 0; i < wrapped.getRowCount(); i++) {
                                for (int j = 0; j < originalDbTable.getRowCount(); j++) {
                                    // (1) per-column debug logs (unchanged detail)
                                    for (Column col : cols) {
                                        String colName = col.getColumnName();
                                        String csvVal =
                                                Optional.ofNullable(wrapped.getValue(i, colName))
                                                        .map(Object::toString).orElse("").trim();
                                        String dbVal = Optional
                                                .ofNullable(originalDbTable.getValue(j, colName))
                                                .map(Object::toString).orElse("").trim();
                                        log.debug(
                                                "[{}] Table[{}] Compare: csvRow={}, dbRow={}, column={}, csv=[{}], db=[{}]",
                                                dbId, table, i, j, colName, csvVal, dbVal);
                                    }
                                    // (2) rowsEqual result
                                    boolean match = rowsEqual(wrapped, originalDbTable, jdbc,
                                            schema, table, i, j, cols, dialectHandler);
                                    log.debug("[{}] Table[{}] rowsEqual(csvRow={}, dbRow={}) => {}",
                                            dbId, table, i, j, match);

                                    if (match) {
                                        identicalMap.put(i, j);
                                        log.debug(
                                                "[{}] Table[{}] Duplicate detected: csvRow={} matches dbRow={}",
                                                dbId, table, i, j);
                                        break;
                                    }
                                }
                            }
                        }

                        // --- 4) DELETE duplicates ---
                        if (!identicalMap.isEmpty()) {
                            String deleteSql;
                            if (!pkCols.isEmpty()) {
                                // DELETE with PK
                                String where = pkCols.stream()
                                        .map(c -> dialectHandler.quoteIdentifier(c) + " = ?")
                                        .collect(Collectors.joining(" AND "));
                                deleteSql = String.format("DELETE FROM %s.%s WHERE %s",
                                        dialectHandler.quoteIdentifier(schema),
                                        dialectHandler.quoteIdentifier(table), where);
                                try (PreparedStatement ps = jdbc.prepareStatement(deleteSql)) {
                                    for (Map.Entry<Integer, Integer> e : identicalMap.entrySet()) {
                                        int dbRow = e.getValue();
                                        // Bind only PK columns
                                        for (int k = 0; k < pkCols.size(); k++) {
                                            String pk = pkCols.get(k);
                                            Object val = originalDbTable.getValue(dbRow, pk);
                                            ps.setObject(k + 1, val);
                                        }
                                        ps.addBatch();
                                    }
                                    int deleted = Arrays.stream(ps.executeBatch()).sum();
                                    log.info(
                                            "[{}] Table[{}] Deleted duplicates by primary key → {}",
                                            dbId, table, deleted);
                                }
                            } else {
                                // DELETE with all columns
                                String where =
                                        Arrays.stream(cols)
                                                .map(c -> dialectHandler.quoteIdentifier(
                                                        c.getColumnName()) + " = ?")
                                                .collect(Collectors.joining(" AND "));
                                deleteSql = String.format("DELETE FROM %s.%s WHERE %s",
                                        dialectHandler.quoteIdentifier(schema),
                                        dialectHandler.quoteIdentifier(table), where);
                                try (PreparedStatement ps = jdbc.prepareStatement(deleteSql)) {
                                    for (Map.Entry<Integer, Integer> e : identicalMap.entrySet()) {
                                        int dbRow = e.getValue();
                                        for (int k = 0; k < cols.length; k++) {
                                            Object val = originalDbTable.getValue(dbRow,
                                                    cols[k].getColumnName());
                                            ps.setObject(k + 1, val);
                                        }
                                        ps.addBatch();
                                    }
                                    int deleted = Arrays.stream(ps.executeBatch()).sum();
                                    log.info(
                                            "[{}] Table[{}] Deleted duplicates by all columns → {}",
                                            dbId, table, deleted);
                                }
                            }
                        }

                        // --- 5) INSERT only new rows ---
                        FilteredTable filtered = new FilteredTable(wrapped, identicalMap.keySet());
                        DefaultDataSet filteredDs = new DefaultDataSet(filtered);
                        DatabaseOperation.INSERT.execute(dbConn, filteredDs);
                        log.info("[{}] Table[{}] Scenario (INSERT only) | inserted={}", dbId, table,
                                filtered.getRowCount());
                    }

                    // Record summary
                    int currentCount = dialectHandler.countRows(jdbc, table);
                    insertSummary.computeIfAbsent(dbId, k -> new LinkedHashMap<>()).put(table,
                            currentCount);
                }

                dbConn.close();
            }

        } catch (Exception e) {
            log.error("[{}] Unexpected error occurred: {}", dbId, e.getMessage(), e);
            ErrorHandler.errorAndExit(errorMessage, e);
        }
    }

    /**
     * Creates a DBUnit {@link DatabaseConnection} from a JDBC {@link Connection}, initializes the
     * session, and applies {@link org.dbunit.dataset.datatype.IDataTypeFactory}.
     *
     * @param jdbc JDBC connection to initialize
     * @param entry connection entry
     * @param dialectHandler DB dialect handler
     * @return configured {@link DatabaseConnection}
     * @throws Exception on errors during session initialization or configuration
     */
    private DatabaseConnection createDbUnitConn(Connection jdbc, ConnectionConfig.Entry entry,
            DbDialectHandler dialectHandler) throws Exception {

        dialectHandler.prepareConnection(jdbc);
        String schema = schemaNameResolver.apply(entry);
        DatabaseConnection dbConn = new DatabaseConnection(jdbc, schema);
        DatabaseConfig config = dbConn.getConfig();
        configFactory.configure(config, dialectHandler.getDataTypeFactory());
        return dbConn;
    }

    /**
     * Generates/regenerates {@code table-ordering.txt}.
     *
     * <p>
     * This method scans the specified directory for supported dataset files (CSV/JSON/YAML/XML) and
     * creates/updates {@code table-ordering.txt} with the list of table names (file base names,
     * sorted).
     * </p>
     *
     * @param dir directory where dataset files are located
     */
    private void ensureTableOrdering(File dir) {
        File orderFile = new File(dir, "table-ordering.txt");

        // List all supported dataset files (CSV, JSON, YAML/YML, XML)
        File[] dataFiles = dir.listFiles((d, name) -> {
            String ext = FilenameUtils.getExtension(name).toLowerCase(Locale.ROOT);
            return DataFormat.CSV.matches(ext) || DataFormat.JSON.matches(ext)
                    || DataFormat.YAML.matches(ext) || DataFormat.XML.matches(ext);
        });
        int fileCount = (dataFiles == null) ? 0 : dataFiles.length;

        // Compute relative path from dataPath
        Path dataDir = Paths.get(pathsConfig.getDataPath()).toAbsolutePath().normalize();
        Path orderPath = orderFile.toPath().toAbsolutePath().normalize();
        String relPath = FilenameUtils.separatorsToUnix(dataDir.relativize(orderPath).toString());

        if (orderFile.exists()) {
            try {
                List<String> lines = Files.readAllLines(orderFile.toPath(), StandardCharsets.UTF_8);
                if (lines.size() == fileCount) {
                    log.info("table-ordering.txt already exists (count matches): {}", relPath);
                    return;
                }
                FileUtils.forceDelete(orderFile);
            } catch (IOException e) {
                FileUtils.deleteQuietly(orderFile);
            }
        }

        if (fileCount == 0) {
            log.info("No dataset files found → ordering file not generated");
            return;
        }

        try {
            String content =
                    Arrays.stream(dataFiles).map(f -> FilenameUtils.getBaseName(f.getName())) // remove
                                                                                              // extension
                            .sorted().collect(Collectors.joining(System.lineSeparator()));
            FileUtils.writeStringToFile(orderFile, content, StandardCharsets.UTF_8);
            log.info("Generated table-ordering.txt: {}", relPath);
        } catch (IOException e) {
            log.error("Failed to create table-ordering.txt: {}", e.getMessage(), e);
            ErrorHandler.errorAndExit("Failed to create table-ordering.txt", e);
        }
    }

    /**
     * Normalizes an INTERVAL DAY TO SECOND string to the CSV dump format {@code +DD HH:MM:SS}.
     *
     * <ul>
     * <li>Sign normalized to leading {@code +} or {@code -} (absent sign becomes {@code +}).</li>
     * <li>Days (DD) are zero-padded to 2 digits.</li>
     * <li>Hours (HH), minutes (MM), and seconds (SS) are each zero-padded to 2 digits.</li>
     * </ul>
     *
     * @param raw raw INTERVAL DAY TO SECOND string (e.g., {@code "+00 5:0:0.0"},
     *        {@code "0 10:2:3"})
     * @return normalized string (e.g., {@code "+00 05:00:00"})
     */
    private String normalizeDaySecondInterval(String raw) {
        if (raw == null) {
            return null;
        }
        // Pattern: [sign][days] SP [hh]:[mm]:[ss](.fraction)?
        Pattern p = Pattern.compile("([+-]?)(\\d+)\\s+(\\d+):(\\d+):(\\d+)(?:\\.\\d+)?");
        Matcher m = p.matcher(raw.trim());
        if (m.matches()) {
            // Add "+" if no sign (everything except "-" becomes "+")
            String sign = m.group(1);
            if (!"-".equals(sign)) {
                sign = "+";
            }
            String dd = String.format("%02d", Integer.parseInt(m.group(2)));
            String hh = String.format("%02d", Integer.parseInt(m.group(3)));
            String mi = String.format("%02d", Integer.parseInt(m.group(4)));
            String ss = String.format("%02d", Integer.parseInt(m.group(5)));
            return sign + dd + " " + hh + ":" + mi + ":" + ss;
        }
        // Fallback: return trimmed original string
        return raw.trim();
    }

    /**
     * Normalizes an INTERVAL YEAR TO MONTH string to the CSV dump format {@code +YY-MM}.
     *
     * <ul>
     * <li>Sign normalized to leading {@code +} or {@code -} (absent sign becomes {@code +}).</li>
     * <li>Years (YY) and months (MM) are zero-padded to 2 digits.</li>
     * </ul>
     *
     * @param raw raw INTERVAL YEAR TO MONTH string (e.g., {@code "1-6"}, {@code "+1-06"},
     *        {@code "-2-3"})
     * @return normalized string (e.g., {@code "+01-06"}, {@code "-02-03"})
     */
    private String normalizeYearMonthInterval(String raw) {
        if (raw == null) {
            return null;
        }
        // Pattern: [sign][years]-[months]
        Pattern p = Pattern.compile("([+-]?)(\\d+)-(\\d+)");
        Matcher m = p.matcher(raw.trim());
        if (m.matches()) {
            // Add "+" if no sign (everything except "-" becomes "+")
            String sign = m.group(1);
            if (!"-".equals(sign)) {
                sign = "+";
            }
            String yy = String.format("%02d", Integer.parseInt(m.group(2)));
            String mm = String.format("%02d", Integer.parseInt(m.group(3)));
            return sign + yy + "-" + mm;
        }
        // Fallback: return trimmed original string
        return raw.trim();
    }

    /**
     * Determines whether the specified rows in two {@link ITable} instances are “equal for CSV
     * display” across all columns.
     *
     * <ul>
     * <li>CSV-side raw value: {@code toString()} → {@code trimToNull()}.</li>
     * <li>DB-side raw value: {@code toString()} → {@code trimToNull()}.</li>
     * <li>INTERVAL YEAR TO MONTH / DAY TO SECOND columns are normalized via
     * {@link #normalizeYearMonthInterval(String)} /
     * {@link #normalizeDaySecondInterval(String)}.</li>
     * <li>Others are formatted via {@code dialectHandler.formatDbValueForCsv()} →
     * {@code trimToNull()}.</li>
     * <li>Returns {@code false} and logs when any mismatch is found.</li>
     * </ul>
     *
     * @param csvTable table from CSV
     * @param dbTable table from DB
     * @param jdbc raw JDBC connection (for type-name lookup)
     * @param schema schema name
     * @param tableName table name (for logging)
     * @param csvRow row index in {@code csvTable} (0-based)
     * @param dbRow row index in {@code dbTable} (0-based)
     * @param cols array of columns to compare
     * @param dialectHandler DB dialect handler
     * @return {@code true} if all columns are equal in terms of CSV presentation
     * @throws DataSetException on DbUnit errors during comparison
     * @throws SQLException on JDBC errors while getting type names or values
     */
    private boolean rowsEqual(ITable csvTable, ITable dbTable, Connection jdbc, String schema,
            String tableName, int csvRow, int dbRow, Column[] cols, DbDialectHandler dialectHandler)
            throws DataSetException, SQLException {

        for (Column col : cols) {
            String colName = col.getColumnName();
            // SQL type name (e.g., "INTERVAL YEAR(2) TO MONTH")
            String typeName = dialectHandler.getColumnTypeName(jdbc, schema, tableName, colName)
                    .toUpperCase(Locale.ROOT);

            // Debug log: type name
            log.debug("Table[{}] Column[{}] Type=[{}]", tableName, colName, typeName);

            // 1) CSV-side raw value → trim-to-null
            String rawCsv = Optional.ofNullable(csvTable.getValue(csvRow, colName))
                    .map(Object::toString).orElse(null);
            String csvCell = StringUtils.trimToNull(rawCsv);

            // 2) DB-side raw value → trim-to-null
            Object rawDbObj = dbTable.getValue(dbRow, colName);
            String rawDb = rawDbObj == null ? null : rawDbObj.toString();
            String dbCell = StringUtils.trimToNull(rawDb);

            // Debug log: values before normalization
            log.debug("Table[{}] Before normalize: csvRow={}, dbRow={}, col={}, csv=[{}], db=[{}]",
                    tableName, csvRow, dbRow, colName, csvCell, dbCell);

            // 3) INTERVAL types always go through normalization
            if (typeName.contains("INTERVAL")) {
                if (typeName.contains("YEAR")) {
                    csvCell = normalizeYearMonthInterval(csvCell);
                    dbCell = normalizeYearMonthInterval(dbCell);
                } else {
                    csvCell = normalizeDaySecondInterval(csvCell);
                    dbCell = normalizeDaySecondInterval(dbCell);
                }
            } else {
                // 4) Others: format via dialect handler, then trim-to-null
                String formatted = dialectHandler.formatDbValueForCsv(colName, rawDbObj);
                dbCell = StringUtils.trimToNull(formatted);
            }

            // Debug log: values after normalization
            log.debug("Table[{}] After normalize: csvRow={}, dbRow={}, col={}, csv=[{}], db=[{}]",
                    tableName, csvRow, dbRow, colName, csvCell, dbCell);

            // 5) Treat null/blank as equal
            if (StringUtils.isAllBlank(csvCell) && StringUtils.isAllBlank(dbCell)) {
                continue;
            }
            // 6) Strict equality; on mismatch, log and return false
            if (!Objects.equals(csvCell, dbCell)) {
                log.debug("Mismatch: Table[{}] csvRow={}, dbRow={}, col={}, csv=[{}], db=[{}]",
                        tableName, csvRow, dbRow, colName, csvCell, dbCell);
                return false;
            }
        }
        return true;
    }

    /**
     * {@link ITable} wrapper that skips specified row indices.<br>
     * Used in scenario mode to exclude rows duplicated from <em>pre</em> when building a table for
     * INSERT.
     */
    private static class FilteredTable implements ITable {

        // The wrapped original {@link ITable} implementation
        private final ITable delegate;

        // Set of row indices to skip
        private final Set<Integer> skipRows;

        /**
         * Creates an instance.
         *
         * @param delegate wrapped {@link ITable} instance
         * @param skipRows set of row indices to exclude (0-based)
         */
        FilteredTable(ITable delegate, Set<Integer> skipRows) {
            this.delegate = delegate;
            this.skipRows = skipRows;
        }

        @Override
        public ITableMetaData getTableMetaData() {
            return delegate.getTableMetaData();
        }

        @Override
        public int getRowCount() {
            return delegate.getRowCount() - skipRows.size();
        }

        @Override
        public Object getValue(int row, String column) throws DataSetException {
            int actual = row;
            for (int i = 0; i <= actual; i++) {
                if (skipRows.contains(i)) {
                    actual++;
                }
            }
            return delegate.getValue(actual, column);
        }
    }

    /**
     * Outputs a consolidated log of data load results for all DBs.
     */
    private void logSummary() {
        log.info("===== Summary =====");
        insertSummary.forEach((dbId, tableMap) -> {
            log.info("DB[{}]:", dbId);
            int maxNameLen = tableMap.keySet().stream().mapToInt(String::length).max().orElse(0);
            int maxCountDigits = tableMap.values().stream().map(cnt -> String.valueOf(cnt).length())
                    .mapToInt(Integer::intValue).max().orElse(0);
            String fmt = "  Table[%-" + maxNameLen + "s] Total=%" + maxCountDigits + "d";
            tableMap.forEach((table, cnt) -> log.info(String.format(fmt, table, cnt)));
        });
        log.info("== Data loading to all DBs has completed ==");
    }

    /**
     * Performs data loading into a single DB using an externally managed JDBC {@link Connection}
     * and the corresponding {@link ConnectionConfig.Entry}. Connection close/rollback is the
     * caller's responsibility.
     *
     * <p>
     * Mode interpretation uses {@link DbUnitConfig#preDirName} as initial (<em>pre</em>). If
     * {@code scenarioName} is {@code null}/empty, use pre; otherwise, use the specified scenario.
     * </p>
     *
     * <p>
     * Excluded tables are applied from {@link DumpConfig#excludeTables}.
     * </p>
     *
     * @param scenarioName scenario name (use pre mode if {@code null}/empty)
     * @param entry target DB connection entry (used for schema/dialect resolution)
     * @param connection open JDBC connection managed by the caller (autoCommit=false recommended)
     * @throws IllegalStateException on fatal errors during load
     */
    public void executeWithConnection(String scenarioName, ConnectionConfig.Entry entry,
            Connection connection) throws SQLException {

        // Determine mode (pre or specified scenario)
        String preMode = dbUnitConfig.getPreDirName();
        String mode = (scenarioName == null || scenarioName.isEmpty()) ? preMode : scenarioName;

        // DB identifier (for logging)
        String dbId = entry.getId();

        log.info("=== DataLoader (external connection) started (mode={}, DB={}) ===", mode, dbId);

        // Directories for initial/pre and scenario
        File initialDir = new File(pathsConfig.getLoad(), preMode + File.separator + dbId);
        File scenarioDir = new File(pathsConfig.getLoad(), mode + File.separator + dbId);

        // Resolve dialect handler
        DbDialectHandler dialectHandler = dialectFactory.apply(entry);

        // pre mode load (if scenarioName is empty/same, the same directory is used)
        deployWithConnection((preMode.equals(mode) ? initialDir : initialDir), dbId, true, entry,
                connection, dialectHandler, "Initial data load failed (DB=" + dbId + ")");

        // additional load for scenario mode only
        if (!preMode.equals(mode)) {
            deployWithConnection(scenarioDir, dbId, false, entry, connection, dialectHandler,
                    "Scenario data load failed (DB=" + dbId + ")");
        }

        log.info("=== DataLoader (external connection) finished (DB={}) ===", dbId);
    }

    /**
     * Executes the same logic as
     * {@link #deploy(File, String, boolean, ConnectionConfig.Entry, DbDialectHandler, String)} but
     * using a caller-supplied external {@link Connection}. Transaction management is the caller's
     * responsibility.
     *
     * <p>
     * This version supports multiple dataset formats (CSV, JSON, YAML/YML, XML).<br>
     * The file format is automatically determined by {@link DataLoaderFactory} based on extension
     * priority (CSV > JSON > YAML/YML > XML).
     * </p>
     *
     * @param dir dataset directory for this DB
     * @param dbId DB identifier (entry.getId())
     * @param initial {@code true} = initial (pre) mode / {@code false} = scenario mode
     * @param entry DB connection entry (schema/dialect resolution)
     * @param jdbc externally managed JDBC connection
     * @param dialectHandler DB dialect handler
     * @param errorMessage message to log on fatal error
     */
    private void deployWithConnection(File dir, String dbId, boolean initial,
            ConnectionConfig.Entry entry, Connection jdbc, DbDialectHandler dialectHandler,
            String errorMessage) {

        if (!dir.exists()) {
            log.warn("[{}] Directory does not exist → skipping", dbId);
            return;
        }

        try {
            // Generate table-ordering file (CSV only, legacy behavior)
            ensureTableOrdering(dir);

            // Collect candidate table names from files
            Set<String> tableSet = new HashSet<>();
            File[] files = dir.listFiles((d, name) -> {
                String ext = FilenameUtils.getExtension(name).toLowerCase(Locale.ROOT);
                // only supported formats
                return Arrays.stream(DataFormat.values()).anyMatch(fmt -> fmt.matches(ext));
            });
            if (files != null) {
                for (File f : files) {
                    String base = FilenameUtils.getBaseName(f.getName());
                    tableSet.add(base);
                }
            }

            List<String> tables = new ArrayList<>(tableSet);
            if (tables.isEmpty()) {
                log.info("[{}] No dataset files → skipping", dbId);
                return;
            }
            tables.sort(String::compareTo);

            // Apply DumpConfig exclusions
            if (dumpConfig != null && dumpConfig.getExcludeTables() != null
                    && !dumpConfig.getExcludeTables().isEmpty()) {
                final Set<String> excludeLower =
                        dumpConfig.getExcludeTables().stream().filter(Objects::nonNull)
                                .map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());

                log.info("[{}] Excluded tables: {}", dbId, excludeLower);

                tables = tables.stream()
                        .filter(t -> !excludeLower.contains(t.toLowerCase(Locale.ROOT)))
                        .collect(Collectors.toList());

                if (tables.isEmpty()) {
                    log.info("[{}] No effective tables (all excluded) → skipping", dbId);
                    return;
                }
            }

            // Create DBUnit connection
            DatabaseConnection dbConn = createDbUnitConn(jdbc, entry, dialectHandler);
            String schema = schemaNameResolver.apply(entry);

            for (String table : tables) {
                // Resolve dataset file for this table (CSV/JSON/YAML/XML)
                IDataSet dataSet;
                try {
                    dataSet = DataLoaderFactory.create(dir, table);
                } catch (Exception e) {
                    log.warn("[{}] Failed to resolve dataset for table={} → skipping: {}", dbId,
                            table, e.getMessage());
                    continue;
                }

                dialectHandler.logTableDefinition(jdbc, schema, table, dbId);

                ITable base = dataSet.getTable(table);

                // Wrap with LOB resolver
                ITable wrapped = new LobResolvingTableWrapper(base, dir, dialectHandler);
                DefaultDataSet ds = new DefaultDataSet(wrapped);

                if (initial) {
                    // Initial mode (CLEAN_INSERT + UPDATE)
                    Column[] lobCols = dialectHandler.getLobColumns(dir.toPath(), table);
                    if (lobCols.length > 0) {
                        boolean anyNotNullLob =
                                dialectHandler.hasNotNullLobColumn(jdbc, schema, table, lobCols);
                        if (anyNotNullLob) {
                            DatabaseOperation.CLEAN_INSERT.execute(dbConn, ds);
                        } else {
                            DatabaseOperation.CLEAN_INSERT.execute(dbConn, new DefaultDataSet(
                                    DefaultColumnFilter.excludedColumnsTable(base, lobCols)));
                            DatabaseOperation.UPDATE.execute(dbConn, ds);
                        }
                    } else {
                        DatabaseOperation.CLEAN_INSERT.execute(dbConn, ds);
                    }
                } else {
                    // Scenario mode (delete duplicates + insert new)
                    ITable originalDbTable = dbConn.createDataSet().getTable(table);
                    List<String> pkCols = dialectHandler.getPrimaryKeyColumns(jdbc, schema, table);

                    Map<Integer, Integer> identicalMap = new LinkedHashMap<>();
                    Column[] cols = wrapped.getTableMetaData().getColumns();

                    if (!pkCols.isEmpty()) {
                        for (int i = 0; i < wrapped.getRowCount(); i++) {
                            for (int j = 0; j < originalDbTable.getRowCount(); j++) {
                                boolean match = true;
                                for (String pk : pkCols) {
                                    Object v1 = wrapped.getValue(i, pk);
                                    Object v2 = originalDbTable.getValue(j, pk);
                                    if (v1 == null ? v2 != null : !v1.equals(v2)) {
                                        match = false;
                                        break;
                                    }
                                }
                                if (match) {
                                    identicalMap.put(i, j);
                                    break;
                                }
                            }
                        }
                    } else {
                        for (int i = 0; i < wrapped.getRowCount(); i++) {
                            for (int j = 0; j < originalDbTable.getRowCount(); j++) {
                                boolean match = rowsEqual(wrapped, originalDbTable, jdbc, schema,
                                        table, i, j, cols, dialectHandler);
                                if (match) {
                                    identicalMap.put(i, j);
                                    break;
                                }
                            }
                        }
                    }

                    if (!identicalMap.isEmpty()) {
                        String deleteSql;
                        if (!pkCols.isEmpty()) {
                            String where = pkCols.stream()
                                    .map(c -> dialectHandler.quoteIdentifier(c) + " = ?")
                                    .collect(Collectors.joining(" AND "));
                            deleteSql = String.format("DELETE FROM %s.%s WHERE %s",
                                    dialectHandler.quoteIdentifier(schema),
                                    dialectHandler.quoteIdentifier(table), where);
                            try (PreparedStatement ps = jdbc.prepareStatement(deleteSql)) {
                                for (Map.Entry<Integer, Integer> e : identicalMap.entrySet()) {
                                    int dbRow = e.getValue();
                                    for (int k = 0; k < pkCols.size(); k++) {
                                        String pk = pkCols.get(k);
                                        Object val = originalDbTable.getValue(dbRow, pk);
                                        ps.setObject(k + 1, val);
                                    }
                                    ps.addBatch();
                                }
                                int deleted = Arrays.stream(ps.executeBatch()).sum();
                                log.info("[{}] Table[{}] Deleted duplicates by primary key → {}",
                                        dbId, table, deleted);
                            }
                        } else {
                            String where = Arrays.stream(cols).map(
                                    c -> dialectHandler.quoteIdentifier(c.getColumnName()) + " = ?")
                                    .collect(Collectors.joining(" AND "));
                            deleteSql = String.format("DELETE FROM %s.%s WHERE %s",
                                    dialectHandler.quoteIdentifier(schema),
                                    dialectHandler.quoteIdentifier(table), where);
                            try (PreparedStatement ps = jdbc.prepareStatement(deleteSql)) {
                                for (Map.Entry<Integer, Integer> e : identicalMap.entrySet()) {
                                    int dbRow = e.getValue();
                                    for (int k = 0; k < cols.length; k++) {
                                        Object val = originalDbTable.getValue(dbRow,
                                                cols[k].getColumnName());
                                        ps.setObject(k + 1, val);
                                    }
                                    ps.addBatch();
                                }
                            }
                        }
                    }

                    FilteredTable filtered = new FilteredTable(wrapped, identicalMap.keySet());
                    DefaultDataSet filteredDs = new DefaultDataSet(filtered);
                    DatabaseOperation.INSERT.execute(dbConn, filteredDs);
                }

                // Update summary
                int currentCount = dialectHandler.countRows(jdbc, table);
                insertSummary.computeIfAbsent(dbId, k -> new LinkedHashMap<>()).put(table,
                        currentCount);
            }

            dbConn.close();

        } catch (Exception e) {
            log.error("[{}] Unexpected error occurred: {}", dbId, e.getMessage(), e);
            ErrorHandler.errorAndExit(errorMessage, e);
        }
    }
}
