package io.github.yok.flexdblink.core;

import com.google.common.io.BaseEncoding;
import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FileNameResolver;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.db.DbDialectHandler;
import io.github.yok.flexdblink.util.CsvUtils;
import io.github.yok.flexdblink.util.ErrorHandler;
import io.github.yok.flexdblink.util.OracleDateTimeFormatUtil;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.dbunit.database.DatabaseConnection;

/**
 * Core class that dumps table data (CSV) and BLOB/CLOB columns (files) from a database in one pass.
 *
 * <p>
 * <strong>Main responsibilities:</strong>
 * </p>
 * <ul>
 * <li>Process multiple DB connections in sequence (via {@link ConnectionConfig}).</li>
 * <li>Export all rows of each table as CSV (<code>dump/{scenario}/{dbId}/{table}.csv</code>).</li>
 * <li>Export all BLOB/CLOB columns under <code>dump/{scenario}/{dbId}/files/</code> and replace the
 * corresponding CSV cell with <code>file:&lt;fileName&gt;</code>.</li>
 * <li>Resolve file names using {@link FileNameResolver} constructed from
 * {@link FilePatternConfig}.</li>
 * </ul>
 *
 * @see FilePatternConfig
 * @see FileNameResolver
 * @see PathsConfig
 * @author Yasuharu.Okawauchi
 */
@Slf4j
@RequiredArgsConstructor
public class DataDumper {

    // Base output directory for dump (mapped from application.yml's dump path via PathsConfig)
    private final PathsConfig pathsConfig;

    // List of DB connection settings (application.yml's connections)
    private final ConnectionConfig connectionConfig;

    // file-patterns configuration (application.yml's file-patterns)
    private final FilePatternConfig filePatternConfig;

    private final DumpConfig dumpConfig;

    // Function to resolve schema name from ConnectionConfig.Entry
    private final Function<ConnectionConfig.Entry, String> schemaNameResolver;

    // Factory function to obtain a DB dialect handler
    // Input : ConnectionConfig.Entry
    // Output: corresponding DbDialectHandler
    private final Function<ConnectionConfig.Entry, DbDialectHandler> dialectFactory;

    // Date/time formatting utility
    private final OracleDateTimeFormatUtil dateTimeFormatter;

    /**
     * Dumps CSV and BLOB/CLOB files for each table according to the specified scenario and target
     * DB list.
     *
     * @param scenario dump scenario name (null/empty is an error)
     * @param targetDbIds list of target DB IDs (if null/empty, all DBs are processed)
     */
    public void execute(String scenario, List<String> targetDbIds) {
        // Validate scenario name
        if (scenario == null || scenario.isEmpty()) {
            ErrorHandler.errorAndExit("Scenario name is required in --dump mode.");
        }

        // Create or check the dump base directory
        File baseDir = new File(pathsConfig.getDump());
        ensureDirectoryExists(baseDir, "Failed to create base output directory");

        // Scenario folder (latest)
        File scenarioDir = new File(baseDir, scenario);

        // ── If the scenario folder already exists, back it up
        if (scenarioDir.exists()) {

            // 2) Backup the existing scenario folder if present
            if (scenarioDir.exists()) {
                try {
                    Path dataPath = Path.of(pathsConfig.getDataPath()).toAbsolutePath().normalize();
                    backupScenarioDirectory(dataPath, scenarioDir);
                } catch (IOException e) {
                    ErrorHandler.errorAndExit(
                            "Exception occurred during backup of the existing scenario directory",
                            e);
                }
            }
        }

        // 3) Create the new scenario folder
        ensureDirectoryExists(scenarioDir, "Failed to create dump output directory");

        // Execute dump per DB
        for (ConnectionConfig.Entry entry : connectionConfig.getConnections()) {
            String dbId = entry.getId();
            // Target DB filter
            if (targetDbIds != null && !targetDbIds.isEmpty() && !targetDbIds.contains(dbId)) {
                log.info("[{}] Skipped: not in target DB list", dbId);
                continue;
            }
            log.info("[{}] === DB dump started ===", dbId);

            // Collect results in LinkedHashMap to keep table iteration order
            Map<String, Integer> summaryMap = new LinkedHashMap<>();

            try {
                // Load JDBC driver
                Class.forName(entry.getDriverClass());
                try (Connection conn = DriverManager.getConnection(entry.getUrl(), entry.getUser(),
                        entry.getPassword())) {

                    // Create dialect handler
                    DbDialectHandler dialectHandler = dialectFactory.apply(entry);

                    // Configure DBUnit connection
                    String schema = schemaNameResolver.apply(entry);
                    DatabaseConnection dbConn = dialectHandler.createDbUnitConnection(conn, schema);

                    // --- Get and filter table list ---
                    List<String> tables =
                            fetchTargetTables(conn, schema, dumpConfig.getExcludeTables());
                    if (tables.isEmpty()) {
                        log.warn("[{}] No tables to dump", dbId);
                        dbConn.close();
                        continue;
                    }

                    // --- Prepare output directories (via common helper) ---
                    File[] dirs = prepareDbOutputDirs(scenarioDir, dbId);
                    File dbDir = dirs[0];
                    File filesDir = dirs[1];

                    FileNameResolver resolver = new FileNameResolver(filePatternConfig);
                    for (String tbl : tables) {
                        // --- 1) CSV dump ---
                        File csvFile = new File(dbDir, tbl + ".csv");
                        exportTableAsCsvUtf8(conn, tbl, csvFile);
                        log.info("[{}] Table[{}] CSV dump completed (UTF-8)", dbId, tbl);

                        // --- 2) BLOB/CLOB dump ---
                        DumpResult result = dumpBlobClob(conn, tbl, dbDir, filesDir, resolver,
                                schema, dialectHandler);
                        log.info("[{}] Table[{}] dumped-records={}, BLOB/CLOB file-outputs={}",
                                dbId, tbl, result.getRowCount(), result.getFileCount());

                        // --- 3) Register to summary map ---
                        summaryMap.put(tbl, result.getRowCount());
                    }
                    // After the dump, output summary
                    logTableSummary(dbId, summaryMap);

                    log.info("[{}] === DB dump completed ===", dbId);

                    dbConn.close();
                }
            } catch (Exception e) {
                ErrorHandler.errorAndExit("Dump failed (DB=" + dbId + ")", e);
            }
        }

        Path dataDir = Paths.get(pathsConfig.getDataPath()).toAbsolutePath().normalize();
        Path scenarioPath = scenarioDir.toPath().toAbsolutePath().normalize();
        String relPath =
                FilenameUtils.separatorsToUnix(dataDir.relativize(scenarioPath).toString());

        log.info("=== All DB dumps completed: Output [{}] ===", relPath);
    }

    /**
     * Creates the following under the specified scenario directory.
     * <ul>
     * <li>1. CSV dump directory ({@code scenarioDir/dbId})</li>
     * <li>2. Subdirectory for BLOB/CLOB files ({@code .../files})</li>
     * </ul>
     * Creates directories if they do not exist; terminates immediately on failure.
     *
     * @param scenarioDir scenario root directory
     * @param dbId DB identifier
     * @return File array: [0] = CSV output directory, [1] = BLOB/CLOB directory
     */
    private File[] prepareDbOutputDirs(File scenarioDir, String dbId) {
        File dbDir = new File(scenarioDir, dbId);
        ensureDirectoryExists(dbDir, "Failed to create DB output directory");
        File filesDir = new File(dbDir, "files");
        ensureDirectoryExists(filesDir, "Failed to create 'files' directory");
        return new File[] {dbDir, filesDir};
    }

    /**
     * Ensures the given directory exists by creating it if missing; terminates via
     * {@link ErrorHandler} if creation fails.
     *
     * @param dir target directory to create
     * @param errorMsg message to display on error (the absolute path is appended)
     */
    private void ensureDirectoryExists(File dir, String errorMsg) {
        if (!dir.exists() && !dir.mkdirs()) {
            ErrorHandler.errorAndExit(errorMsg + ": " + dir.getAbsolutePath());
        }
    }

    /**
     * Renames the existing scenario output directory to a timestamped backup directory.<br>
     * Backup folder name format: {@code <originalName>_yyyyMMddHHmmssSSS}.<br>
     * Also logs the paths relative to {@code dataPath} using UNIX separators.
     *
     * @param dataPath the {@code dataPath} specified in application.yml
     * @param scenarioDir scenario directory to back up
     * @throws IOException on rename/path manipulation errors
     */
    private void backupScenarioDirectory(Path dataPath, File scenarioDir) throws IOException {
        // Generate timestamped folder name
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"));
        File backupDir =
                scenarioDir.toPath().getParent().resolve(scenarioDir.getName() + "_" + ts).toFile();

        // Perform rename
        if (!scenarioDir.renameTo(backupDir)) {
            ErrorHandler.errorAndExit("Failed to back up the existing output directory: "
                    + scenarioDir.getAbsolutePath());
        }

        // Compute relative paths and log
        String relOrig = FilenameUtils
                .separatorsToUnix(dataPath.relativize(scenarioDir.toPath()).toString());
        String relBackup =
                FilenameUtils.separatorsToUnix(dataPath.relativize(backupDir.toPath()).toString());
        log.info("Backed up existing dump output directory: {} → {}", relOrig, relBackup);
    }

    /**
     * Retrieves all table names in the specified schema and filters out excluded ones.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param excludeTables list of table names to exclude (case-insensitive)
     * @return list of target table names after filtering
     * @throws SQLException on SQL error while retrieving tables
     */
    private List<String> fetchTargetTables(Connection conn, String schema,
            List<String> excludeTables) throws SQLException {
        List<String> tables = new ArrayList<>();
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, schema, "%", new String[] {"TABLE"})) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                boolean excluded =
                        excludeTables.stream().anyMatch(ex -> ex.equalsIgnoreCase(tableName));
                if (excluded) {
                    log.info("Table [{}] is excluded; skipping", tableName);
                } else {
                    tables.add(tableName);
                }
            }
        }
        return tables;
    }

    /**
     * Executes {@code SELECT *} for the specified table and writes the result to a UTF-8 CSV
     * file.<br>
     * If a primary key exists, rows are sorted in ascending order by the PK columns; otherwise,
     * rows are sorted by the leftmost column. Sorting is performed in-memory before writing
     * CSV.<br>
     * RAW/LONG RAW/VARBINARY-like types are emitted as hexadecimal strings.
     *
     * @param conn JDBC connection
     * @param table table name
     * @param csvFile destination CSV file
     * @throws Exception on SQL or file I/O error
     */
    private void exportTableAsCsvUtf8(Connection conn, String table, File csvFile)
            throws Exception {

        // --- 1) Extract primary key column names from metadata ---
        List<String> pkColumns = fetchPrimaryKeyColumns(conn, conn.getSchema(), table);

        // --- 2) Read header first (for sorting) ---
        String[] headerArray;
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " WHERE 1=0")) {
            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();
            headerArray = new String[colCount];
            for (int i = 1; i <= colCount; i++) {
                headerArray[i - 1] = md.getColumnLabel(i);
            }
        }

        // --- 3) Build SELECT with ORDER BY (explicit column names) ---
        String orderBy;
        if (!pkColumns.isEmpty()) {
            // Attach "ASC" to each PK column
            String cols =
                    pkColumns.stream().map(col -> col + " ASC").collect(Collectors.joining(", "));
            orderBy = " ORDER BY " + cols;
            log.debug("Table[{}] Sorting by primary key(s) (SQL): {}", table, cols);
        } else {
            // Fallback to first column
            String firstCol = headerArray[0];
            orderBy = " ORDER BY " + firstCol + " ASC";
            log.debug("Table[{}] No primary key -> sort by first column '{}' (SQL)", table,
                    firstCol);
        }
        String sql = "SELECT * FROM " + table + orderBy;

        // --- 4) Read records into memory ---
        List<List<String>> rows = new ArrayList<>();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();

            while (rs.next()) {
                List<String> row = new ArrayList<>(colCount);
                for (int i = 1; i <= colCount; i++) {
                    int sqlType = md.getColumnType(i);
                    String typeNm = md.getColumnTypeName(i);
                    String cell;
                    // RAW / LONG RAW / VARBINARY → hex string
                    if ("RAW".equalsIgnoreCase(typeNm) || "LONG RAW".equalsIgnoreCase(typeNm)
                            || sqlType == Types.BINARY || sqlType == Types.VARBINARY
                            || sqlType == Types.LONGVARBINARY
                            || (sqlType == Types.OTHER && "RAW".equalsIgnoreCase(typeNm))) {
                        byte[] bytes = rs.getBytes(i);
                        cell = (bytes == null || bytes.length == 0) ? ""
                                : org.apache.commons.codec.binary.Hex.encodeHexString(bytes)
                                        .toUpperCase();
                    } else {
                        Object val = rs.getObject(i);
                        cell = (val == null) ? "" : val.toString();
                    }
                    row.add(cell);
                }
                rows.add(row);
            }
        }

        // --- 5) Sort in-memory (use extracted indices) ---
        List<Integer> sortIdx = buildSortIndices(headerArray, pkColumns);

        rows.sort((a, b) -> {
            for (int idx : sortIdx) {
                // Trim whitespace before comparison
                String sa = StringUtils.trimToEmpty(a.get(idx));
                String sb = StringUtils.trimToEmpty(b.get(idx));
                int cmp;
                try {
                    // Numeric compare when both look like integers
                    cmp = Integer.compare(Integer.parseInt(sa), Integer.parseInt(sb));
                } catch (NumberFormatException ex) {
                    // Fallback to string compare
                    cmp = sa.compareTo(sb);
                }
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        });

        // --- 6) Write sorted data to CSV ---
        CsvUtils.writeCsvUtf8(csvFile, headerArray, rows);
    }

    /**
     * Retrieves the list of primary key column names for the specified table.
     *
     * @param conn JDBC connection
     * @param schema schema name
     * @param table table name
     * @return list of PK column names (empty if none)
     * @throws SQLException on SQL error while reading PK metadata
     */
    private List<String> fetchPrimaryKeyColumns(Connection conn, String schema, String table)
            throws SQLException {
        List<String> pkColumns = new ArrayList<>();
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getPrimaryKeys(conn.getCatalog(), schema, table)) {
            while (rs.next()) {
                pkColumns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return pkColumns;
    }

    /**
     * Builds the list of column indices to use for sorting, given the header array and the PK
     * column list.
     *
     * @param headers CSV header names
     * @param pkColumns PK column names (if empty, the first column is used)
     * @return list of indices to sort by
     */
    private List<Integer> buildSortIndices(String[] headers, List<String> pkColumns) {
        // Map header name → index
        Map<String, Integer> headerIndex = IntStream.range(0, headers.length).boxed().collect(
                Collectors.toMap(i -> headers[i], i -> i, (a, b) -> a, LinkedHashMap::new));

        if (pkColumns.isEmpty()) {
            // No PK → sort by the first column
            return List.of(0);
        }
        // Extract indices in PK order
        return pkColumns.stream().map(headerIndex::get).collect(Collectors.toList());
    }

    /**
     * Scans all rows of the specified table, formats values by type, writes BLOB/CLOB columns to
     * files, and replaces the cell text in the CSV. Finally sorts by PK (or first column)
     * ascending, and overwrites the CSV.
     *
     * @param conn JDBC connection
     * @param table table name
     * @param dbDir directory for CSV files
     * @param filesDir directory for BLOB/CLOB files
     * @param resolver utility for resolving file names
     * @param schema schema name
     * @param dialectHandler implementation of DbDialectHandler (incl. OracleDialectHandler)
     * @return dump result (CSV row count + number of files written)
     * @throws Exception on SQL or file I/O error
     */
    private DumpResult dumpBlobClob(Connection conn, String table, File dbDir, File filesDir,
            FileNameResolver resolver, String schema, DbDialectHandler dialectHandler)
            throws Exception {
        int fileCount = 0;

        // --- Read CSV ---
        File csvFile = new File(dbDir, table + ".csv");
        if (!csvFile.exists()) {
            log.warn("CSV file not found: {}", csvFile.getAbsolutePath());
            return new DumpResult(0, 0);
        }

        // 1) Parse header row
        CSVFormat headerFmt = CSVFormat.DEFAULT.builder().setSkipHeaderRecord(false).setTrim(false)
                .setQuote('"').setQuoteMode(QuoteMode.MINIMAL).get();
        List<String> headers = new ArrayList<>();
        try (CSVParser p = CSVParser.parse(csvFile, StandardCharsets.UTF_8, headerFmt)) {
            CSVRecord hr = p.iterator().next();
            for (String h : hr) {
                headers.add(h.trim().replaceAll("^\"|\"$", ""));
            }
        }

        // 2) Parse data rows
        CSVFormat dataFmt = CSVFormat.DEFAULT.builder().setHeader(headers.toArray(new String[0]))
                .setSkipHeaderRecord(true).setTrim(false).setQuote('"')
                .setQuoteMode(QuoteMode.MINIMAL).setEscape('\\').get();
        List<CSVRecord> records;
        try (CSVParser p = CSVParser.parse(csvFile, StandardCharsets.UTF_8, dataFmt)) {
            records = p.getRecords();
        }

        // 3) Copy to memory
        List<List<String>> csvData = new ArrayList<>();
        for (CSVRecord rec : records) {
            List<String> row = new ArrayList<>();
            for (String h : headers) {
                row.add(rec.get(h));
            }
            csvData.add(row);
        }

        // 4) Log BLOB/CLOB patterns
        Map<String, String> tablePatterns = filePatternConfig.getPatternsForTable(table);
        if (tablePatterns == null) {
            throw new IllegalStateException(
                    "No definition for \"" + table + "\" found in file-patterns.");
        }
        String joined = tablePatterns.entrySet().stream()
                .map(e -> e.getKey() + " : " + e.getValue()).collect(Collectors.joining(", "));
        log.debug("BLOB/CLOB output filename patterns: [{}]", joined);

        // 5) Scan DB + write files + replace cells
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {

            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();

            int rowIndex = 0;
            while (rs.next()) {
                List<String> row = csvData.get(rowIndex++);
                boolean wroteAny = false;

                for (int i = 1; i <= colCount; i++) {
                    String col = md.getColumnLabel(i);
                    int type = md.getColumnType(i);
                    int idx = headers.indexOf(col);
                    if (idx < 0) {
                        continue;
                    }

                    Object raw = rs.getObject(i);
                    String cell;

                    // DATE/TIMESTAMP/INTERVAL types
                    if (isOracleIntervalColumn(col)) {
                        String sv = rs.getString(i);
                        cell = dateTimeFormatter.formatJdbcDateTime(col, sv, conn);

                    } else if (raw != null && (type == Types.DATE || type == Types.TIMESTAMP
                            || type == -101 || type == -102)) {
                        cell = dateTimeFormatter.formatJdbcDateTime(col, raw, conn);

                        // BLOB/CLOB types
                    } else if (raw != null
                            && (type == Types.BLOB || type == Types.CLOB || type == Types.NCLOB)) {
                        String pattern = filePatternConfig.getPattern(table, col)
                                .orElseThrow(() -> new IllegalStateException("No definition for \""
                                        + table + "\" / \"" + col + "\" in file-patterns."));
                        Map<String, Object> keyMap = buildKeyMap(rs, pattern);
                        String fname = applyPlaceholders(pattern, keyMap);
                        Path outPath = filesDir.toPath().resolve(fname);
                        dialectHandler.writeLobFile(table, col, raw, outPath);
                        wroteAny = true;
                        cell = "file:" + fname;

                        // RAW/BINARY family
                    } else if (raw != null && (type == Types.BINARY || type == Types.VARBINARY
                            || type == Types.LONGVARBINARY)) {
                        byte[] bytes = rs.getBytes(i);
                        cell = BaseEncoding.base16().upperCase().encode(bytes);

                        // Others
                    } else {
                        String v = rs.getString(i);
                        cell = (v == null || "null".equals(v)) ? "" : v;
                    }

                    row.set(idx, cell);
                    if (wroteAny) {
                        fileCount++;
                        wroteAny = false;
                    }
                }
            }
        }

        // 6) Compute sort-key indices
        List<String> pkColumns = new ArrayList<>();
        try (ResultSet pkRs = conn.getMetaData().getPrimaryKeys(conn.getCatalog(), schema, table)) {
            while (pkRs.next()) {
                pkColumns.add(pkRs.getString("COLUMN_NAME"));
            }
        }
        Map<String, Integer> headerIndex = IntStream.range(0, headers.size()).boxed().collect(
                Collectors.toMap(i -> headers.get(i), i -> i, (a, b) -> a, LinkedHashMap::new));
        List<Integer> sortIdx = !pkColumns.isEmpty()
                ? pkColumns.stream().map(headerIndex::get).collect(Collectors.toList())
                : List.of(0);

        // 7) Sort csvData ascending (trim → numeric-or-string compare)
        csvData.sort((a, b) -> {
            for (int idx : sortIdx) {
                String va = StringUtils.trimToEmpty(a.get(idx));
                String vb = StringUtils.trimToEmpty(b.get(idx));
                int cmp;
                try {
                    cmp = Integer.compare(Integer.parseInt(va), Integer.parseInt(vb));
                } catch (NumberFormatException ex) {
                    cmp = va.compareTo(vb);
                }
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        });

        // 8) Overwrite CSV
        CSVFormat outFmt = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get();
        try (Writer w =
                new OutputStreamWriter(new FileOutputStream(csvFile), StandardCharsets.UTF_8);
                CSVPrinter pr = new CSVPrinter(w, outFmt)) {
            pr.printRecord(headers);
            for (List<String> row : csvData) {
                pr.printRecord(row);
            }
        }

        return new DumpResult(csvData.size(), fileCount);
    }

    /**
     * Determines whether the column name corresponds to an Oracle INTERVAL type.
     * <ul>
     * <li>INTERVAL_YM_COL</li>
     * <li>INTERVAL_DS_COL</li>
     * </ul>
     *
     * @param col column name to check
     * @return {@code true} if it looks like an INTERVAL column name; {@code false} otherwise
     */
    private boolean isOracleIntervalColumn(String col) {
        return "INTERVAL_YM_COL".equalsIgnoreCase(col) || "INTERVAL_DS_COL".equalsIgnoreCase(col);
    }

    /**
     * Extracts placeholders like {@code {COLUMN}} from {@code rawPattern} using a regular
     * expression, retrieves the values from the {@link java.sql.ResultSet} for the current row with
     * matching column names, and returns a map of placeholders to values.
     *
     * <p>
     * Even for tables without a primary key, any column that appears in {@code file-patterns} can
     * be used as a key.
     * </p>
     *
     * @param rs JDBC result set (current row is the target)
     * @param rawPattern file name pattern (e.g., {@code "tbl_{COL1}_{COL2}.bin"})
     * @return map of placeholder name → column value
     * @throws java.sql.SQLException on column access error
     */
    private Map<String, Object> buildKeyMap(ResultSet rs, String rawPattern) throws SQLException {
        Map<String, Object> keyMap = new HashMap<>();
        Matcher m = Pattern.compile("\\{(.+?)\\}").matcher(rawPattern);
        while (m.find()) {
            String col = m.group(1);
            keyMap.put(col, rs.getObject(col));
        }
        return keyMap;
    }

    /**
     * Replaces placeholders in the specified pattern with values from the provided key-value
     * map.<br>
     * Placeholders are written as {@code {COLUMN_NAME}} and replaced with the corresponding entry
     * in {@code keyMap}.
     *
     * <pre>
     * // Example:
     * String pattern = "LeapSecond_{ID}_{TIMESTAMP}.dat";
     * Map&lt;String, Object&gt; keyMap = Map.of("ID", 3, "TIMESTAMP", "20250630T235959");
     * // Result: "LeapSecond_3_20250630T235959.dat"
     * </pre>
     *
     * @param pattern template string containing placeholders (e.g.,
     *        {@code "tbl_{COL1}_{COL2}.bin"})
     * @param keyMap map keyed by placeholder names with replacement values
     * @return string with all placeholders replaced
     * @throws NullPointerException if any key or value is {@code null}
     */
    private String applyPlaceholders(String pattern, Map<String, Object> keyMap) {
        String result = pattern;
        for (Map.Entry<String, Object> e : keyMap.entrySet()) {
            String placeholder = "{" + e.getKey() + "}";
            if (result.contains(placeholder)) {
                result = result.replace(placeholder, Objects.toString(e.getValue()));
            }
        }
        return result;
    }

    /**
     * Logs a formatted summary of per-table dump results for the given DB.
     *
     * @param dbId DB identifier
     * @param tableCountMap map of table name → record count
     */
    private void logTableSummary(String dbId, Map<String, Integer> tableCountMap) {
        log.info("===== Summary =====");
        log.info("DB[{}]:", dbId);

        // Compute the maximum table-name length
        int maxNameLen = tableCountMap.keySet().stream().mapToInt(String::length).max().orElse(0);

        // Compute the maximum digit width for counts
        int maxCountDigits =
                tableCountMap.values().stream().map(count -> String.valueOf(count).length())
                        .mapToInt(Integer::intValue).max().orElse(0);

        // Example: " Table[%-20s] Total=%5d"
        String fmt = "  Table[%-" + maxNameLen + "s] Total=%" + maxCountDigits + "d";

        // Format and log each table
        tableCountMap.forEach((table, count) -> log.info(String.format(fmt, table, count)));
    }
}
