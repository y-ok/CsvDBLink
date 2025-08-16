package io.github.yok.csvdblink.db;

import io.github.yok.csvdblink.config.ConnectionConfig;
import io.github.yok.csvdblink.config.DbUnitConfig;
import io.github.yok.csvdblink.config.DumpConfig;
import io.github.yok.csvdblink.config.PathsConfig;
import io.github.yok.csvdblink.util.OracleDateTimeFormatUtil;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.springframework.stereotype.Component;

/**
 * Factory class that creates a {@link DbDialectHandler} according to the database type.
 *
 * <p>
 * The selected handler is based on {@link DbUnitConfig}'s DataTypeFactoryMode. Currently this
 * factory creates an {@link OracleDialectHandler} for the ORACLE mode. In addition, dump-related
 * settings such as {@link DumpConfig#excludeTables} are passed to the handler.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DbDialectHandlerFactory {

    // DBUnit settings (includes dataTypeFactoryMode, lobDirName, preDirName)
    private final DbUnitConfig dbUnitConfig;

    // Dump settings (e.g., exclude-tables)
    private final DumpConfig dumpConfig;

    // Builds load/dump directory paths from data-path
    private final PathsConfig pathsConfig;

    // Date/time formatter utility for CSV handling
    private final OracleDateTimeFormatUtil dateTimeFormatter;

    // Applies common settings to DBUnit's DatabaseConfig
    private final DbUnitConfigFactory configFactory;

    /**
     * Creates a {@link DbDialectHandler} based on the provided connection entry.
     *
     * <p>
     * Supported DataTypeFactoryMode:
     * </p>
     * <ul>
     * <li>{@code ORACLE}: instantiate {@link OracleDialectHandler}</li>
     * </ul>
     *
     * @param entry connection information (URL, user, password, ID, etc.)
     * @return a configured instance of {@link DbDialectHandler}
     * @throws IllegalStateException if handler creation or initialization fails
     */
    public DbDialectHandler create(ConnectionConfig.Entry entry) {
        try {
            switch (dbUnitConfig.getDataTypeFactoryMode()) {
                case ORACLE:
                    // 1) Establish a raw JDBC connection
                    Connection jdbc = DriverManager.getConnection(entry.getUrl(), entry.getUser(),
                            entry.getPassword());

                    // 2) Create a DBUnit connection (schema uses the upper-cased user name)
                    DatabaseConnection dbConn =
                            new DatabaseConnection(jdbc, entry.getUser().toUpperCase());

                    // 3) Create and initialize OracleDialectHandler
                    OracleDialectHandler handler = new OracleDialectHandler(dbConn, dumpConfig,
                            dbUnitConfig, configFactory, dateTimeFormatter, pathsConfig);

                    // 4) Apply common DBUnit configuration
                    DatabaseConfig config = dbConn.getConfig();
                    configFactory.configure(config, handler.getDataTypeFactory());

                    // 5) Perform dialect/session initialization
                    handler.prepareConnection(jdbc);

                    return handler;

                default:
                    String msg = "Unsupported DataTypeFactoryMode: "
                            + dbUnitConfig.getDataTypeFactoryMode();
                    log.error(msg);
                    throw new IllegalArgumentException(msg);
            }

        } catch (SQLException e) {
            log.error("Failed to establish JDBC connection or initialize session", e);
            throw new IllegalStateException("Failed to connect to the database", e);

        } catch (DataSetException e) {
            log.error("Failed to initialize DBUnit dataset/connection", e);
            throw new IllegalStateException("Failed to initialize DBUnit dataset", e);

        } catch (Exception e) {
            log.error("Unexpected error during DbDialectHandler creation", e);
            throw new IllegalStateException("Failed to create DbDialectHandler", e);
        }
    }
}
