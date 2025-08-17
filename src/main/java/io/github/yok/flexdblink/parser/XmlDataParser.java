package io.github.yok.flexdblink.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.nio.file.Files;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;

/**
 * Implementation of {@link DataParser} that reads XML dataset files from a directory and produces a
 * {@link IDataSet}.
 *
 * <p>
 * Each XML file should be in DBUnit's {@code FlatXmlDataSet} format. The file name (without
 * extension) is used as the table name implicitly by DBUnit.
 * </p>
 */
public class XmlDataParser implements DataParser {

    @Override
    public IDataSet parse(File dir) throws Exception {
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File d, String name) {
                return name.toLowerCase().endsWith(".xml");
            }
        });
        if (files == null || files.length == 0) {
            throw new DataSetException("No XML files found in directory: " + dir);
        }

        // For now, load the first XML dataset file
        File file = files[0];
        if (!Files.isReadable(file.toPath())) {
            throw new DataSetException("XML file is not readable: " + file);
        }

        return new FlatXmlDataSetBuilder().setColumnSensing(true).build(new FileInputStream(file));
    }
}
