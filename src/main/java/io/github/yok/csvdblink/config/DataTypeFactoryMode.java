package io.github.yok.csvdblink.config;

/**
 * Enumerates the selection modes for {@code IDataTypeFactory} and
 * {@link io.github.yok.csvdblink.db.DbDialectHandler} used by DBUnit.
 *
 * <p>
 * Each constant selects the handler and data type factory corresponding to a specific database
 * product.
 * </p>
 *
 * <ul>
 * <li>ORACLE — for Oracle Database</li>
 * <li>POSTGRESQL — for PostgreSQL</li>
 * <li>MYSQL — for MySQL</li>
 * <li>SQLSERVER — for Microsoft SQL Server</li>
 * </ul>
 *
 * @author Yasuharu.Okawauchi
 */
public enum DataTypeFactoryMode {
    // Use the handler and DataTypeFactory for Oracle DB
    ORACLE,
    // Use the handler and DataTypeFactory for PostgreSQL
    POSTGRESQL,
    // Use the handler and DataTypeFactory for MySQL
    MYSQL,
    // Use the handler and DataTypeFactory for Microsoft SQL Server
    SQLSERVER
}
