package io.github.yok.csvdblink.db;

import io.github.yok.csvdblink.config.DbUnitConfigProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.springframework.stereotype.Component;

/**
 * Factory class that centrally applies application-wide settings to DBUnit's
 * {@link DatabaseConfig}.
 *
 * <p>
 * This class bundles common DBUnit settings—such as the data type factory, allowance of empty
 * fields, batched statement execution, and batch size—and applies them in one place. Core classes
 * only need to call this factory for configuration to be completed.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DbUnitConfigFactory {

    // Properties class that externalizes DBUnit settings
    private final DbUnitConfigProperties props;

    /**
     * No-args constructor.
     *
     * <p>
     * Provides simple initialization for use outside the Spring container (e.g., JUnit extensions),
     * using a {@link DbUnitConfigProperties} instance with default values.
     * </p>
     */
    public DbUnitConfigFactory() {
        this.props = new DbUnitConfigProperties();
    }

    /**
     * Applies application-wide settings to the specified {@link DatabaseConfig}.
     *
     * @param cfg DBUnit {@link DatabaseConfig} object
     * @param dataTypeFactory vendor-specific {@link IDataTypeFactory} implementation
     */
    public void configure(DatabaseConfig cfg, IDataTypeFactory dataTypeFactory) {
        // 1) Set the data type factory
        cfg.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, dataTypeFactory);
        log.debug("DBUnit: DataTypeFactory set to {}", dataTypeFactory.getClass().getSimpleName());

        // 2) Configure whether to allow empty fields ("")
        cfg.setProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, props.isAllowEmptyFields());
        log.debug("DBUnit: allow empty fields = {}", props.isAllowEmptyFields());

        // 3) Configure whether to enable batched statements execution
        cfg.setProperty(DatabaseConfig.FEATURE_BATCHED_STATEMENTS, props.isBatchedStatements());
        log.debug("DBUnit: batched statements enabled = {}", props.isBatchedStatements());

        // 4) Configure batch size
        cfg.setProperty(DatabaseConfig.PROPERTY_BATCH_SIZE, props.getBatchSize());
        log.debug("DBUnit: batch size = {}", props.getBatchSize());
    }
}
