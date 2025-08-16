package io.github.yok.csvdblink.db;

import io.github.yok.csvdblink.config.ConnectionConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.IDataTypeFactory;

/**
 * Handler interface that abstracts per-database-dialect behavior and provides CSV↔DB mappings,
 * connection/session preparation, and LOB processing.
 *
 * <p>
 * Implementations encapsulate vendor-specific logic such as identifier quoting, pagination syntax,
 * date/time formatting and parsing, sequence/identity support, and DBUnit {@link IDataTypeFactory}
 * selection.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
public interface DbDialectHandler {

    /**
     * Returns the SQL type name for the specified schema/table/column.
     *
     * @param jdbc raw JDBC {@link Connection}
     * @param schema schema name
     * @param table table name
     * @param column column name
     * @return SQL type name
     * @throws SQLException if metadata retrieval fails
     */
    String getColumnTypeName(Connection jdbc, String schema, String table, String column)
            throws SQLException;

    /**
     * Retrieves the list of primary-key column names for a table.
     *
     * <p>
     * Returns an empty list when no primary key is defined. For composite keys, the list is ordered
     * by the definition order (KEY_SEQ).
     * </p>
     *
     * @param conn JDBC connection
     * @param schema schema name (case rules may be DB-dependent)
     * @param table table name (case rules may be DB-dependent)
     * @return ordered list of primary-key column names (empty if none)
     * @throws SQLException if metadata retrieval fails
     */
    List<String> getPrimaryKeyColumns(Connection conn, String schema, String table)
            throws SQLException;

    /**
     * Applies dialect-specific initialization to a JDBC {@link Connection}.
     *
     * <p>
     * Examples include search-path/schema settings or session variables.
     * </p>
     *
     * @param connection JDBC connection to initialize
     * @throws SQLException if an SQL error occurs during initialization
     */
    void prepareConnection(Connection connection) throws SQLException;

    /**
     * Converts a CSV string value into a placeholder/object suitable for JDBC binding and DBUnit,
     * based on the table/column type, precision/scale, etc.
     *
     * <p>
     * Implementations should handle nulls and empty strings appropriately.
     * </p>
     *
     * @param table table name
     * @param column column name
     * @param value CSV string value
     * @return JDBC-bindable object (e.g., Date, Timestamp, Boolean)
     * @throws DataSetException if the value cannot be parsed/converted
     */
    Object convertCsvValueToDbType(String table, String column, String value)
            throws DataSetException;

    /**
     * Formats a value obtained from JDBC/DB into a CSV string representation.
     *
     * @param columnName column name
     * @param value JDBC value (Date, Timestamp, CLOB/BLOB, etc.)
     * @return string to be written to CSV
     * @throws SQLException if DB access is required during formatting and fails
     */
    String formatDbValueForCsv(String columnName, Object value) throws SQLException;

    /**
     * Resolves the schema name to use from a {@link ConnectionConfig.Entry}.
     *
     * @param entry connection-config entry
     * @return schema name to use (with appropriate case)
     */
    String resolveSchema(ConnectionConfig.Entry entry);

    /**
     * Creates a DBUnit {@link DatabaseConnection} using the given JDBC {@link Connection} and
     * schema.
     *
     * @param connection JDBC connection
     * @param schema schema name
     * @return initialized {@link DatabaseConnection}
     * @throws Exception if creation/initialization fails
     */
    DatabaseConnection createDbUnitConnection(Connection connection, String schema)
            throws Exception;

    /**
     * Writes a LOB column value to a file.
     *
     * @param schema schema name
     * @param table table name
     * @param value LOB object
     * @param outputPath destination file path
     * @throws Exception on file I/O or JDBC-related failures
     */
    void writeLobFile(String schema, String table, Object value, Path outputPath) throws Exception;

    /**
     * Reads a previously written LOB file and converts it into a JDBC-bindable object.
     *
     * @param schema schema name
     * @param table table name
     * @param column column name
     * @param file LOB file to read
     * @return JDBC-bindable object (e.g., CLOB/BLOB)
     * @throws IOException if file reading fails
     * @throws DataSetException if the file contents are invalid for the target column
     */
    Object readLobFile(String schema, String table, String column, File file)
            throws IOException, DataSetException;

    /**
     * Indicates whether the dialect supports LOB processing via streaming APIs.
     *
     * @return {@code true} if LOBs can be processed using streaming APIs
     */
    boolean supportsLobStreamByStream();

    /**
     * Applies custom formatting when converting date/time columns to strings.
     *
     * @param columnName column name
     * @param value date/time object obtained from JDBC
     * @param connection JDBC connection (for timezone/session info, if needed)
     * @return CSV-friendly date/time string
     * @throws SQLException if an error occurs during formatting
     */
    String formatDateTimeColumn(String columnName, Object value, Connection connection)
            throws SQLException;

    /**
     * Parses a CSV date/time string and converts it into a JDBC-bindable object.
     *
     * @param columnName column name
     * @param value CSV date/time string
     * @return JDBC-bindable date/time object
     * @throws Exception if parsing fails
     */
    Object parseDateTimeValue(String columnName, String value) throws Exception;

    /**
     * Returns SQL to obtain the next value of a sequence.
     *
     * @param sequenceName sequence name
     * @return SQL to fetch the next sequence value
     */
    String getNextSequenceSql(String sequenceName);

    /**
     * Returns SQL used to retrieve an auto-generated key after INSERT.
     *
     * @return SQL for generated-key retrieval
     */
    String getGeneratedKeyRetrievalSql();

    /**
     * Indicates whether JDBC {@code getGeneratedKeys()} is supported.
     *
     * @return {@code true} if keys can be retrieved directly; {@code false} if another SQL is
     *         required
     */
    boolean supportsGetGeneratedKeys();

    /**
     * Indicates whether sequences are supported.
     *
     * @return {@code true} if sequence features can be used
     */
    boolean supportsSequences();

    /**
     * Indicates whether IDENTITY columns are supported.
     *
     * @return {@code true} if IDENTITY INSERT or equivalent is available
     */
    boolean supportsIdentityColumns();

    /**
     * Applies dialect-specific pagination to a SELECT statement.
     *
     * @param baseSql base SELECT SQL
     * @param offset number of rows to skip
     * @param limit maximum number of rows to fetch
     * @return SELECT SQL with pagination per dialect rules
     */
    String applyPagination(String baseSql, int offset, int limit);

    /**
     * Quotes an identifier (table name, column name, etc.) according to the dialect.
     *
     * @param identifier identifier to quote
     * @return quoted identifier
     */
    String quoteIdentifier(String identifier);

    /**
     * Returns the literal used by the dialect to represent boolean {@code true}.
     *
     * @return TRUE literal
     */
    String getBooleanTrueLiteral();

    /**
     * Returns the literal used by the dialect to represent boolean {@code false}.
     *
     * @return FALSE literal
     */
    String getBooleanFalseLiteral();

    /**
     * Returns the SQL function/expression to obtain the current timestamp.
     *
     * @return dialect-specific expression for current timestamp
     */
    String getCurrentTimestampFunction();

    /**
     * Formats a {@link LocalDateTime} as a SQL date/time literal.
     *
     * @param dateTime {@link LocalDateTime} instance
     * @return date/time literal to embed in SQL
     */
    String formatDateLiteral(LocalDateTime dateTime);

    /**
     * Builds an UPSERT statement (MERGE / INSERT ON DUPLICATE KEY UPDATE, etc.) per dialect.
     *
     * @param tableName table name
     * @param keyColumns list of key column names
     * @param insertColumns list of columns for INSERT
     * @param updateColumns list of columns for UPDATE
     * @return dialect-specific UPSERT SQL
     */
    String buildUpsertSql(String tableName, List<String> keyColumns, List<String> insertColumns,
            List<String> updateColumns);

    /**
     * Returns SQL to create a temporary table.
     *
     * @param tempTableName temporary table name
     * @param columns map of column name → type literal
     * @return CREATE TEMP TABLE SQL
     */
    String getCreateTempTableSql(String tempTableName, Map<String, String> columns);

    /**
     * Applies a SELECT ... FOR UPDATE clause according to the dialect.
     *
     * @param baseSql base SELECT SQL
     * @return SELECT SQL with FOR UPDATE clause
     */
    String applyForUpdate(String baseSql);

    /**
     * Indicates whether batch updates (PreparedStatement.addBatch/executeBatch) are supported.
     *
     * @return {@code true} if batch updates are supported
     */
    boolean supportsBatchUpdates();

    /**
     * Returns the {@link IDataTypeFactory} used by this handler for DBUnit.
     *
     * @return DBUnit data type factory
     */
    IDataTypeFactory getDataTypeFactory();

    /**
     * Checks whether the specified table contains any NOT NULL LOB columns.
     *
     * @param connection JDBC connection
     * @param schema schema name
     * @param table table name
     * @param columns column definitions
     * @return {@code true} if a NOT NULL LOB column exists
     * @throws SQLException if metadata retrieval fails
     */
    boolean hasNotNullLobColumn(Connection connection, String schema, String table,
            Column[] columns) throws SQLException;

    /**
     * Determines whether the specified table has a primary key defined.
     *
     * @param connection JDBC connection
     * @param schema schema name
     * @param table table name
     * @return {@code true} if the table has a primary key
     * @throws SQLException if metadata retrieval fails
     */
    boolean hasPrimaryKey(Connection connection, String schema, String table) throws SQLException;

    /**
     * Counts the total number of rows in a table.
     *
     * @param connection JDBC connection
     * @param table table name
     * @return row count
     * @throws SQLException if SQL execution fails
     */
    int countRows(Connection connection, String table) throws SQLException;

    /**
     * Retrieves LOB column definitions for the table.
     *
     * @param dataDir base directory (where LOB files are stored)
     * @param table table name
     * @return array of LOB columns
     * @throws IOException if an error occurs while constructing file paths
     * @throws DataSetException if metadata reading fails
     */
    Column[] getLobColumns(Path dataDir, String table) throws IOException, DataSetException;

    /**
     * Logs the table definition (column names and types).
     *
     * @param connection JDBC connection
     * @param schema schema name
     * @param table table name
     * @param loggerName SLF4J logger name
     * @throws SQLException if metadata retrieval fails
     */
    void logTableDefinition(Connection connection, String schema, String table, String loggerName)
            throws SQLException;
}
