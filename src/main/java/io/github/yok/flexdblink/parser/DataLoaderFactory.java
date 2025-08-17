package io.github.yok.flexdblink.parser;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.dbunit.dataset.IDataSet;

/**
 * Factory class for resolving and creating {@link IDataSet} instances based on table names and file
 * formats.
 *
 * <p>
 * This factory inspects the given scenario directory and attempts to locate a dataset file whose
 * base name matches the target table name (case-insensitive). When multiple formats exist, the
 * following priority order is applied:
 * </p>
 *
 * <ol>
 * <li>CSV</li>
 * <li>JSON</li>
 * <li>YAML/YML</li>
 * <li>XML</li>
 * </ol>
 *
 * <p>
 * The first matching file according to this order is used, and all others are skipped.
 * </p>
 *
 * <p>
 * Currently only CSV parsing is implemented. Parsers for JSON, YAML, and XML will be added in later
 * steps.
 * </p>
 */
@Slf4j
public class DataLoaderFactory {

    // Priority order of data formats when multiple files exist.
    private static final List<DataFormat> FORMAT_PRIORITY =
            Arrays.asList(DataFormat.CSV, DataFormat.JSON, DataFormat.YAML, DataFormat.XML);

    /**
     * Creates an {@link IDataSet} for the given table name by resolving an appropriate file within
     * the scenario directory.
     *
     * @param scenarioDir the root directory containing dataset files for the scenario
     * @param tableName the logical table name to match (case-insensitive)
     * @return an {@link IDataSet} parsed from the resolved file
     * @throws Exception if no suitable file is found or parsing fails
     */
    public static IDataSet create(File scenarioDir, String tableName) throws Exception {
        String targetName = tableName.toLowerCase(Locale.ROOT);

        for (DataFormat format : FORMAT_PRIORITY) {
            File[] matches = scenarioDir.listFiles((dir, name) -> {
                String base = FilenameUtils.getBaseName(name).toLowerCase(Locale.ROOT);
                String ext = FilenameUtils.getExtension(name).toLowerCase(Locale.ROOT);
                return base.equals(targetName) && format.matches(ext);
            });

            if (matches != null && matches.length > 0) {
                File candidate = matches[0];
                log.info("Resolved dataset file: {}", candidate.getName());
                return createParser(format).parse(scenarioDir);
            }
        }

        throw new IllegalArgumentException("No dataset file found for table: " + tableName);
    }

    /**
     * Creates the appropriate {@link DataParser} for the given {@link DataFormat}.
     *
     * <p>
     * Supported formats are:
     * <ul>
     * <li>{@link DataFormat#CSV}</li>
     * <li>{@link DataFormat#JSON}</li>
     * <li>{@link DataFormat#YAML}</li>
     * <li>{@link DataFormat#XML}</li>
     * </ul>
     * </p>
     *
     * @param format the {@link DataFormat} to create a parser for
     * @return a {@link DataParser} implementation corresponding to the format
     * @throws IllegalArgumentException if the format is not supported
     */
    private static DataParser createParser(DataFormat format) {
        switch (format) {
            case CSV:
                return new CsvDataParser();
            case JSON:
                return new JsonDataParser();
            case YAML:
                return new YamlDataParser();
            case XML:
                return new XmlDataParser();
            default:
                throw new IllegalArgumentException("Unsupported format: " + format);
        }
    }
}
