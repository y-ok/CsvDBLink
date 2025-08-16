package io.github.yok.csvdblink.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration class that binds the {@code dbunit} section in {@code application.yml}, such as the
 * DBUnit operation mode and the LOB file directory. Centralizes the behavior of {@code DataLoader}
 * and {@code DataDumper}.
 *
 * <p>
 * <strong>Supported modes:</strong>
 * </p>
 * <ul>
 * <li>{@link DataTypeFactoryMode#ORACLE}: Use the factory for Oracle</li>
 * <li>{@link DataTypeFactoryMode#POSTGRESQL}: Use the factory for PostgreSQL</li>
 * <li>{@link DataTypeFactoryMode#MYSQL}: Use the factory for MySQL</li>
 * <li>{@link DataTypeFactoryMode#SQLSERVER}: Use the factory for SQL Server</li>
 * </ul>
 *
 * @author Yasuharu.Okawauchi
 */
@Component
@ConfigurationProperties(prefix = "dbunit")
@Data
public class DbUnitConfig {

    /**
     * Specifies the operation mode of the DataTypeFactory. One of the following values can be set:
     *
     * <ul>
     * <li>{@link DataTypeFactoryMode#ORACLE}: Use the factory for Oracle</li>
     * <li>{@link DataTypeFactoryMode#POSTGRESQL}: Use the factory for PostgreSQL</li>
     * <li>{@link DataTypeFactoryMode#MYSQL}: Use the factory for MySQL</li>
     * <li>{@link DataTypeFactoryMode#SQLSERVER}: Use the factory for SQL Server</li>
     * </ul>
     */
    private DataTypeFactoryMode dataTypeFactoryMode = DataTypeFactoryMode.ORACLE;

    /**
     * Directory name that stores LOB files (BLOB/CLOB).
     */
    private String lobDirName = "files";

    /**
     * Directory name used for initial data loading.
     */
    private String preDirName = "pre";
}
