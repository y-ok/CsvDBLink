package io.github.yok.csvdblink;

import io.github.yok.csvdblink.config.ConnectionConfig;
import io.github.yok.csvdblink.config.DbUnitConfig;
import io.github.yok.csvdblink.config.DumpConfig;
import io.github.yok.csvdblink.config.FilePatternConfig;
import io.github.yok.csvdblink.config.PathsConfig;
import io.github.yok.csvdblink.core.DataDumper;
import io.github.yok.csvdblink.core.DataLoader;
import io.github.yok.csvdblink.db.DbDialectHandler;
import io.github.yok.csvdblink.db.DbDialectHandlerFactory;
import io.github.yok.csvdblink.db.DbUnitConfigFactory;
import io.github.yok.csvdblink.util.ErrorHandler;
import io.github.yok.csvdblink.util.OracleDateTimeFormatUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Provides the application entry point.
 *
 * <p>
 * Parses the command-line options {@code --load}/{@code -l}, {@code --dump}/{@code -d}, and
 * {@code --target}/{@code -t}, then invokes {@link DataLoader} or {@link DataDumper}.
 * </p>
 *
 * <p>
 * Argument specification:
 * </p>
 * <ul>
 * <li>{@code --load [scenario]} or {@code -l [scenario]} enables <em>load</em> mode. If the
 * scenario name is omitted, the {@code preDirName} setting in {@code application.yml} is used.</li>
 * <li>{@code --dump [scenario]} or {@code -d [scenario]} enables <em>dump</em> mode. The scenario
 * name is required.</li>
 * <li>{@code --target [db1,db2,…]} or {@code -t [db1,db2,…]} specifies the target DB ID list. If
 * omitted, all DBs are targeted.</li>
 * </ul>
 *
 * <p>
 * The DB dialect handler is created via {@link DbDialectHandlerFactory}.<br>
 * According to {@code dbunit.dataTypeFactoryMode} in {@code application.yml}, a
 * {@link io.github.yok.csvdblink.db.DbDialectHandler} for Oracle / PostgreSQL / MySQL / SQL Server
 * is selected.
 * </p>
 *
 * <p>
 * Spring Boot automatically loads {@link PathsConfig}, {@link DbUnitConfig},
 * {@link ConnectionConfig}, and {@link FilePatternConfig} and passes them to {@link DataLoader} and
 * {@link DataDumper}.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 * @see PathsConfig
 * @see DbUnitConfig
 * @see ConnectionConfig
 * @see FilePatternConfig
 * @see DbDialectHandlerFactory
 * @see OracleDateTimeFormatUtil
 */
@Slf4j
@SpringBootApplication
@EnableConfigurationProperties({PathsConfig.class, DbUnitConfig.class, ConnectionConfig.class,
        FilePatternConfig.class, DumpConfig.class})
@RequiredArgsConstructor
public class Main implements CommandLineRunner {

    private final PathsConfig pathsConfig;
    private final DbUnitConfig dbUnitConfig;
    private final ConnectionConfig connectionConfig;
    private final FilePatternConfig filePatternConfig;
    private final DumpConfig dumpConfig;
    private final DbDialectHandlerFactory dialectFactory;
    private final OracleDateTimeFormatUtil dateTimeFormatter;
    private final DbUnitConfigFactory configFactory;

    /**
     * Bootstraps the application.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(Main.class);
        app.setAddCommandLineProperties(false);
        app.run(args);
    }

    /**
     * Entry point invoked after Spring Boot starts.
     *
     * <p>
     * Parses arguments and controls the execution of {@link DataLoader} or {@link DataDumper}.
     * </p>
     *
     * @param args command-line arguments array
     */
    @Override
    public void run(String... args) {
        log.info("Application started. Args: {}", Arrays.toString(args));

        // Parse CLI arguments
        String mode = null;
        String scenario = null;
        List<String> targetDbIds = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--load":
                case "-l":
                    mode = "load";
                    scenario = (i + 1 < args.length ? args[++i] : null);
                    break;
                case "--dump":
                case "-d":
                    mode = "dump";
                    scenario = (i + 1 < args.length ? args[++i] : null);
                    break;
                case "--target":
                case "-t":
                    if (i + 1 < args.length) {
                        targetDbIds = Arrays.stream(args[++i].split(",")).map(String::trim)
                                .collect(Collectors.toList());
                    }
                    break;
                default:
                    log.warn("Unknown argument: {}", args[i]);
            }
        }

        // Defaults
        if (mode == null) {
            mode = "load";
            scenario = dbUnitConfig.getPreDirName();
        }
        if ("dump".equals(mode) && (scenario == null || scenario.isEmpty())) {
            ErrorHandler.errorAndExit("Scenario name is required in dump mode.");
        }

        // Additional tweak: when --target is not specified (empty list), use all DB IDs from
        // application.yml
        if (targetDbIds.isEmpty()) {
            targetDbIds = connectionConfig.getConnections().stream()
                    .map(ConnectionConfig.Entry::getId).collect(Collectors.toList());
        }

        log.info("Mode: {}, Scenario: {}, Target DBs: {}", mode, scenario, targetDbIds);

        // Schema resolver logic
        Function<ConnectionConfig.Entry, String> schemaResolver =
                entry -> entry.getUser().toUpperCase();

        Function<ConnectionConfig.Entry, DbDialectHandler> dialectHandlerProvider =
                dialectFactory::create;

        // Execute
        try {
            if ("load".equals(mode)) {
                log.info("Starting data load. Scenario [{}], Target DBs {}", scenario, targetDbIds);

                new DataLoader(pathsConfig, connectionConfig, schemaResolver,
                        dialectHandlerProvider, dbUnitConfig, configFactory, dumpConfig)
                                .execute(scenario, targetDbIds);

                log.info("Data load completed. Scenario [{}]", scenario);

            } else {
                log.info("Starting data dump. Scenario [{}], Target DBs [{}]", scenario,
                        targetDbIds);
                new DataDumper(pathsConfig, connectionConfig, filePatternConfig, dumpConfig,
                        schemaResolver, dialectHandlerProvider, dateTimeFormatter).execute(scenario,
                                targetDbIds);
                log.info("Data dump completed. Scenario [{}]", scenario);
            }

        } catch (Exception e) {
            log.error("Fatal error occurred (mode={}): {}", mode, e.getMessage(), e);
            ErrorHandler.errorAndExit("Fatal error: " + e.getMessage(), e);
        }
    }
}
