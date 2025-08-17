package io.github.yok.flexdblink.parser;

import java.io.File;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.csv.CsvDataSet;

/**
 * Implementation of {@link DataParser} that reads a directory of CSV files and produces a
 * {@link CsvDataSet}.
 */
public class CsvDataParser implements DataParser {

    @Override
    public IDataSet parse(File dir) throws Exception {
        return new CsvDataSet(dir);
    }
}
