package io.github.yok.flexdblink.core;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.db.LobResolvingTableWrapper;
import io.github.yok.flexdblink.parser.DataFormat;
import io.github.yok.flexdblink.parser.DataLoaderFactory;
import io.github.yok.flexdblink.util.ErrorHandler;
import io.github.yok.flexdblink.util.LogPathUtil;
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
import org.dbunit.database.IDatabaseConnection;
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
     * Reads dataset (CSV/JSON/YAML/XML + LOB) from the specified directory.
     *
     * <p>
     * In <em>initial</em> mode, performs CLEAN_INSERT + UPDATE.<br>
     * In <em>scenario</em> mode, deletes rows that are duplicates of initial and INSERTs the
     * remainder.
     * </p>
     *
     * @param dir directory where dataset files are located
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
            // Ensure table-ordering.txt exists
            ensureTableOrdering(dir);

            // Load table list from table-ordering.txt
            Path orderPath = new File(dir, "table-ordering.txt").toPath();
            List<String> tables =
                    Files.readAllLines(orderPath, StandardCharsets.UTF_8).stream().map(String::trim)
                            .filter(s -> !s.isEmpty()).sorted().collect(Collectors.toList());

            if (tables.isEmpty()) {
                log.info("[{}] No tables → skipping", dbId);
                return;
            }

            // Exclude tables if configured
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

            Class.forName(entry.getDriverClass());
            try (Connection jdbc = DriverManager.getConnection(entry.getUrl(), entry.getUser(),
                    entry.getPassword())) {

                DatabaseConnection dbConn = createDbUnitConn(jdbc, entry, dialectHandler);
                String schema = schemaNameResolver.apply(entry);

                for (String table : tables) {
                    try {
                        // ファイル形式を自動判別して読み込み
                        IDataSet dataSet = DataLoaderFactory.create(dir, table);

                        ITable base = dataSet.getTable(table);
                        int rowCount = base.getRowCount();
                        log.info("[{}] Table[{}] rows={}", dbId, table, rowCount);

                        ITable wrapped = new LobResolvingTableWrapper(base, dir, dialectHandler);
                        DefaultDataSet ds = new DefaultDataSet(wrapped);

                        if (initial) {
                            // --- Initial mode (CLEAN_INSERT + UPDATE) ---
                            Column[] lobCols = dialectHandler.getLobColumns(dir.toPath(), table);
                            if (lobCols.length > 0) {
                                boolean anyNotNullLob = dialectHandler.hasNotNullLobColumn(jdbc,
                                        schema, table, lobCols);
                                if (anyNotNullLob) {
                                    log.info(
                                            "[{}] Table[{}] NOT NULL LOB present → CLEAN_INSERT all columns",
                                            dbId, table);
                                    DatabaseOperation.CLEAN_INSERT.execute(dbConn, ds);
                                } else {
                                    DatabaseOperation.CLEAN_INSERT.execute(dbConn,
                                            new DefaultDataSet(DefaultColumnFilter
                                                    .excludedColumnsTable(base, lobCols)));
                                    DatabaseOperation.UPDATE.execute(dbConn, ds);
                                }
                            } else {
                                DatabaseOperation.CLEAN_INSERT.execute(dbConn, ds);
                            }
                            log.info("[{}] Table[{}] Initial | inserted={}", dbId, table, rowCount);

                        } else {
                            // --- Scenario mode (delete duplicates + INSERT new) ---
                            ITable originalDbTable = dbConn.createDataSet().getTable(table);
                            List<String> pkCols =
                                    dialectHandler.getPrimaryKeyColumns(jdbc, schema, table);

                            Map<Integer, Integer> identicalMap = detectDuplicates(wrapped,
                                    originalDbTable, pkCols, jdbc, schema, table, dialectHandler);

                            if (!identicalMap.isEmpty()) {
                                deleteDuplicates(jdbc, schema, table, pkCols,
                                        wrapped.getTableMetaData().getColumns(), originalDbTable,
                                        identicalMap, dialectHandler, dbId);
                            }

                            FilteredTable filtered =
                                    new FilteredTable(wrapped, identicalMap.keySet());
                            DatabaseOperation.INSERT.execute(dbConn, new DefaultDataSet(filtered));
                            log.info("[{}] Table[{}] Scenario (INSERT only) | inserted={}", dbId,
                                    table, filtered.getRowCount());
                        }

                        // Summary
                        int currentCount = dialectHandler.countRows(jdbc, table);
                        insertSummary.computeIfAbsent(dbId, k -> new LinkedHashMap<>()).put(table,
                                currentCount);

                    } catch (Exception e) {
                        log.warn("[{}] Failed to load table [{}]: {}", dbId, table, e.getMessage(),
                                e);
                    }
                }

                dbConn.close();
            }

        } catch (Exception e) {
            log.error("[{}] Unexpected error occurred: {}", dbId, e.getMessage(), e);
            ErrorHandler.errorAndExit(errorMessage, e);
        }
    }

    /**
     * Detects duplicate rows between dataset and DB table.
     *
     * @param wrapped dataset table with potential duplicates
     * @param originalDbTable snapshot of DB table
     * @param pkCols list of primary key columns (empty if no PK)
     * @param jdbc JDBC connection
     * @param schema schema name
     * @param table table name
     * @param dialectHandler DB dialect handler
     * @return map of dataset row index → DB row index for duplicates
     * @throws DataSetException if detection fails
     */
    private Map<Integer, Integer> detectDuplicates(ITable wrapped, ITable originalDbTable,
            List<String> pkCols, Connection jdbc, String schema, String table,
            DbDialectHandler dialectHandler) throws DataSetException {

        Map<Integer, Integer> identicalMap = new LinkedHashMap<>();
        try {
            Column[] cols = wrapped.getTableMetaData().getColumns();

            if (!pkCols.isEmpty()) {
                // With PK
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
                                    schema, table, i, j);
                            break;
                        }
                    }
                }
            } else {
                // Without PK → compare all columns
                for (int i = 0; i < wrapped.getRowCount(); i++) {
                    for (int j = 0; j < originalDbTable.getRowCount(); j++) {
                        boolean match = rowsEqual(wrapped, originalDbTable, jdbc, schema, table, i,
                                j, cols, dialectHandler);
                        if (match) {
                            identicalMap.put(i, j);
                            log.debug(
                                    "[{}] Table[{}] Duplicate detected: csvRow={} matches dbRow={}",
                                    schema, table, i, j);
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new DataSetException("Failed to detect duplicates for table: " + table, e);
        }
        return identicalMap;
    }

    /**
     * Deletes duplicate rows from DB using the duplicate map.
     *
     * @param jdbc JDBC connection
     * @param schema schema name
     * @param table table name
     * @param pkCols primary key columns
     * @param cols dataset column metadata
     * @param originalDbTable snapshot of DB table
     * @param identicalMap dataset→DB duplicate mapping
     * @param dialectHandler DB dialect handler
     * @param dbId logical DB identifier
     * @throws DataSetException if deletion fails
     */
    private void deleteDuplicates(Connection jdbc, String schema, String table, List<String> pkCols,
            Column[] cols, ITable originalDbTable, Map<Integer, Integer> identicalMap,
            DbDialectHandler dialectHandler, String dbId) throws DataSetException {

        if (identicalMap.isEmpty()) {
            return;
        }

        try {
            if (!pkCols.isEmpty()) {
                // DELETE by PK
                String where = pkCols.stream().map(c -> dialectHandler.quoteIdentifier(c) + " = ?")
                        .collect(Collectors.joining(" AND "));
                String deleteSql = String.format("DELETE FROM %s.%s WHERE %s",
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
                    log.info("[{}] Table[{}] Deleted duplicates by primary key {}", dbId, table,
                            deleted);
                }
            } else {
                // DELETE by all columns
                String where = Arrays.stream(cols)
                        .map(c -> dialectHandler.quoteIdentifier(c.getColumnName()) + " = ?")
                        .collect(Collectors.joining(" AND "));
                String deleteSql = String.format("DELETE FROM %s.%s WHERE %s",
                        dialectHandler.quoteIdentifier(schema),
                        dialectHandler.quoteIdentifier(table), where);

                try (PreparedStatement ps = jdbc.prepareStatement(deleteSql)) {
                    for (Map.Entry<Integer, Integer> e : identicalMap.entrySet()) {
                        int dbRow = e.getValue();
                        for (int k = 0; k < cols.length; k++) {
                            Object val = originalDbTable.getValue(dbRow, cols[k].getColumnName());
                            ps.setObject(k + 1, val);
                        }
                        ps.addBatch();
                    }
                    int deleted = Arrays.stream(ps.executeBatch()).sum();
                    log.info("[{}] Table[{}] Deleted duplicates by all columns → {}", dbId, table,
                            deleted);
                }
            }
        } catch (SQLException e) {
            throw new DataSetException("Failed to delete duplicates for table: " + table, e);
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
                    Arrays.stream(dataFiles).map(f -> FilenameUtils.getBaseName(f.getName()))
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
     * Loads all datasets (CSV / JSON / YAML/YML / XML) under the specified directory into a single
     * database. No "pre" or "scenario" modes are handled here.
     *
     * <p>
     * This method uses a caller-managed external JDBC {@link Connection}. The caller is responsible
     * for transaction control (commit/rollback/close).
     * </p>
     *
     * <p>
     * Load strategy is always equivalent to an "initial load": by default it performs
     * {@code CLEAN_INSERT}. If LOB columns exist and all of them are NULL-allowed, it first
     * performs {@code CLEAN_INSERT} for non-LOB columns, then applies {@code UPDATE} to reflect LOB
     * values. Excluded tables are taken from {@link DumpConfig#excludeTables}.
     * </p>
     *
     * @param dir target directory that contains table files (one file per table)
     * @param entry DB connection entry (used for schema/dialect resolution)
     * @param connection external JDBC connection managed by the caller (autoCommit=false
     *        recommended)
     * @throws SQLException if a database access error occurs
     * @throws IllegalArgumentException if any argument is null
     * @throws IllegalStateException if {@code dir} does not exist or is not a directory
     */
    public void executeWithConnection(File dir, ConnectionConfig.Entry entry, Connection connection)
            throws SQLException {

        // Fail fast on invalid arguments
        if (dir == null) {
            throw new IllegalArgumentException("Target directory must not be null.");
        }
        if (entry == null) {
            throw new IllegalArgumentException("Connection entry must not be null.");
        }
        if (connection == null) {
            throw new IllegalArgumentException("JDBC connection must not be null.");
        }

        final String dbId = entry.getId();
        log.info("=== DataLoader (external connection) START (db={}, dir={}) ===", dbId,
                LogPathUtil.renderDirForLog(dir));

        // Strict existence check (no silent skip)
        if (!dir.exists() || !dir.isDirectory()) {
            log.error("[{}] Target directory does not exist or is not a directory: {}", dbId,
                    dir.getAbsolutePath());
            throw new IllegalStateException(
                    "Target directory does not exist or is not a directory: "
                            + dir.getAbsolutePath());
        }

        // Resolve dialect handler
        DbDialectHandler dialectHandler = dialectFactory.apply(entry);

        // Execute actual loading with a single directory (no mode concept)
        deployWithConnection(dir, dbId, entry, connection, dialectHandler,
                "Data load failed (db=" + dbId + ")");

        log.info("=== DataLoader (external connection) END (db={}) ===", dbId);
    }

    /**
     * Uses the caller-managed JDBC {@link Connection} to load all tables under a single directory,
     * applying an "initial load" strategy (CLEAN_INSERT by default, with a LOB-specific exception).
     *
     * <p>
     * This method does not perform any "pre/scenario" branching. Only supported file formats within
     * the directory (CSV / JSON / YAML/YML / XML) are considered.
     * </p>
     *
     * @param dir dataset directory for this DB
     * @param dbId DB identifier used for logging (typically {@code entry.getId()})
     * @param entry DB connection entry (used for schema/dialect resolution)
     * @param jdbc caller-managed JDBC connection
     * @param dialectHandler DB dialect handler
     * @param errorMessage message passed to {@link ErrorHandler} on fatal errors
     * @throws IllegalStateException if {@code dir} does not exist or is not a directory
     */
    private void deployWithConnection(File dir, String dbId, ConnectionConfig.Entry entry,
            Connection jdbc, DbDialectHandler dialectHandler, String errorMessage) {

        // Defensive check (callers should have validated already)
        if (!dir.exists() || !dir.isDirectory()) {
            log.error("[{}] Target directory does not exist or is not a directory: {}", dbId,
                    dir.getAbsolutePath());
            throw new IllegalStateException(
                    "Target directory does not exist or is not a directory: "
                            + dir.getAbsolutePath());
        }

        IDatabaseConnection dbConn = null;
        try {
            // Generate table-ordering file for CSV (legacy behavior)
            ensureTableOrdering(dir);

            // Collect candidate table names from files (only supported extensions)
            Set<String> tableSet = new HashSet<>();
            File[] files = dir.listFiles((d, name) -> {
                String ext = FilenameUtils.getExtension(name).toLowerCase(Locale.ROOT);
                return Arrays.stream(DataFormat.values()).anyMatch(fmt -> fmt.matches(ext));
            });
            if (files != null) {
                for (File f : files) {
                    // file base name = table name
                    String base = FilenameUtils.getBaseName(f.getName());
                    tableSet.add(base);
                }
            }

            List<String> tables = new ArrayList<>(tableSet);
            if (tables.isEmpty()) {
                log.info("[{}] No dataset files found under: {}", dbId, dir.getAbsolutePath());
                return;
            }
            // Stable alphabetical order
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
                    log.info("[{}] No effective tables remain after exclusions.", dbId);
                    return;
                }
            }

            // Create DBUnit connection
            dbConn = createDbUnitConn(jdbc, entry, dialectHandler);
            String schema = schemaNameResolver.apply(entry);

            // Load each table (always "initial load" strategy)
            for (String table : tables) {
                // Resolve dataset for the table (CSV / JSON / YAML / XML)
                IDataSet dataSet;
                try {
                    dataSet = DataLoaderFactory.create(dir, table);
                } catch (Exception e) {
                    // Skip only this table if resolution fails
                    log.warn("[{}] Failed to resolve dataset for table={} — skipping: {}", dbId,
                            table, e.getMessage());
                    continue;
                }

                // Log table definition details via dialect handler (DDL, PKs, etc.)
                dialectHandler.logTableDefinition(jdbc, schema, table, dbId);

                // Wrap with LOB resolver (expands file:... references)
                ITable base = dataSet.getTable(table);
                ITable wrapped = new LobResolvingTableWrapper(base, dir, dialectHandler);
                DefaultDataSet ds = new DefaultDataSet(wrapped);

                // Determine LOB handling strategy
                Column[] lobCols = dialectHandler.getLobColumns(dir.toPath(), table);
                if (lobCols.length > 0) {
                    boolean anyNotNullLob =
                            dialectHandler.hasNotNullLobColumn(jdbc, schema, table, lobCols);
                    if (anyNotNullLob) {
                        // LOB contains NOT NULL → single CLEAN_INSERT
                        DatabaseOperation.CLEAN_INSERT.execute(dbConn, ds);
                    } else {
                        // All LOB columns are NULL-allowed:
                        // 1) CLEAN_INSERT for non-LOB columns
                        ITable nonLobOnly = DefaultColumnFilter.excludedColumnsTable(base, lobCols);
                        DatabaseOperation.CLEAN_INSERT.execute(dbConn,
                                new DefaultDataSet(nonLobOnly));
                        // 2) UPDATE to reflect LOB values (including file:... sources)
                        DatabaseOperation.UPDATE.execute(dbConn, ds);
                    }
                } else {
                    // No LOB columns → simple CLEAN_INSERT
                    DatabaseOperation.CLEAN_INSERT.execute(dbConn, ds);
                }

                // Update summary
                int currentCount = dialectHandler.countRows(jdbc, table);
                insertSummary.computeIfAbsent(dbId, k -> new LinkedHashMap<>()).put(table,
                        currentCount);

                log.info("[{}] Loaded table successfully: {} (current rows={})", dbId, table,
                        currentCount);
            }

        } catch (Exception e) {
            log.error("[{}] Unexpected error occurred: {}", dbId, e.getMessage(), e);
            ErrorHandler.errorAndExit(errorMessage, e);
        } finally {
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (Exception closeEx) {
                    log.warn("[{}] Failed to close DBUnit connection: {}", dbId,
                            closeEx.getMessage(), closeEx);
                }
            }
        }
    }
}
