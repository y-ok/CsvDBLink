package io.github.yok.flexdblink.junit;

import io.github.yok.flexdblink.parser.DataLoaderFactory;
import java.io.File;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.Assertion;
import org.dbunit.assertion.DiffCollectingFailureHandler;
import org.dbunit.assertion.Difference;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.filter.DefaultColumnFilter;

/**
 * Provides assertion utilities for comparing database contents against expected dataset files.
 *
 * <p>
 * Supported formats: CSV, JSON, YAML, XML. The expected files must be placed under
 * {@code scenario/expected/}, with file names matching table names.
 * </p>
 */
@Slf4j
public final class ExpectedDataAssert {

    private ExpectedDataAssert() {
        // Utility class, prevent instantiation
    }

    /**
     * Asserts that the actual database contents match the expected dataset.
     *
     * @param jdbc JDBC connection
     * @param schema schema name
     * @param expectedDir directory containing expected dataset files
     * @param excludeColumns columns to be excluded from comparison (case-insensitive)
     * @throws Exception if comparison fails
     */
    public static void assertMatches(Connection jdbc, String schema, File expectedDir,
            String[] excludeColumns) throws Exception {

        DatabaseConnection dbConn = null;
        try {
            dbConn = new DatabaseConnection(jdbc, schema);
            IDataSet actualDataSet = dbConn.createDataSet();

            // Build exclusion set for case-insensitive column filtering
            Set<String> excludeSet = new HashSet<>();
            if (excludeColumns != null) {
                excludeSet.addAll(Arrays.asList(excludeColumns));
            }

            for (String table : actualDataSet.getTableNames()) {
                try {
                    // Load expected dataset file for this table (auto-detect format)
                    IDataSet expectedDataSet = DataLoaderFactory.create(expectedDir, table);
                    ITable expectedTable = expectedDataSet.getTable(table);
                    ITable actualTable = actualDataSet.getTable(table);

                    // Apply exclusion filter if needed
                    if (!excludeSet.isEmpty()) {
                        Column[] actualCols = actualTable.getTableMetaData().getColumns();
                        Column[] excluded = Arrays.stream(actualCols)
                                .filter(c -> excludeSet.contains(c.getColumnName().toUpperCase()))
                                .toArray(Column[]::new);

                        actualTable =
                                DefaultColumnFilter.excludedColumnsTable(actualTable, excluded);
                        expectedTable =
                                DefaultColumnFilter.excludedColumnsTable(expectedTable, excluded);
                    }

                    log.info("Asserting table [{}] (excludeColumns={})", table, excludeSet);

                    // Use a failure handler to collect differences
                    DiffCollectingFailureHandler diffHandler = new DiffCollectingFailureHandler();
                    Assertion.assertEquals(expectedTable, actualTable, diffHandler);

                    // If differences exist, log them in detail and fail
                    if (!diffHandler.getDiffList().isEmpty()) {
                        for (Object obj : diffHandler.getDiffList()) {
                            Difference diff = (Difference) obj; // 明示キャスト
                            log.error(
                                    "Mismatch in table [{}], row={}, column={} | expected=[{}], actual=[{}]",
                                    table, diff.getRowIndex(), diff.getColumnName(),
                                    diff.getExpectedValue(), diff.getActualValue());
                        }
                        throw new AssertionError("Differences detected in table: " + table
                                + " (see logs for details)");
                    }
                } catch (IllegalArgumentException e) {
                    // No expected file found for this table → skip silently
                    log.debug("No expected dataset found for table [{}], skipping", table);
                }
            }
        } finally {
            if (dbConn != null) {
                dbConn.close(); // Explicit close (IDatabaseConnection is not AutoCloseable)
            }
        }
    }
}
