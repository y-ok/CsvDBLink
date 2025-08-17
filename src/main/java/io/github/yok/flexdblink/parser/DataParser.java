package io.github.yok.flexdblink.parser;

import java.io.File;
import org.dbunit.dataset.IDataSet;

/**
 * Interface for parsing a data directory (including multiple files) and producing a DBUnit
 * {@link IDataSet}.
 */
public interface DataParser {

    /**
     * Parses the specified data directory and generates a {@link IDataSet}.
     *
     * @param dir directory containing the data files
     * @return the parsed {@link IDataSet}
     * @throws Exception if parsing fails
     */
    IDataSet parse(File dir) throws Exception;
}
