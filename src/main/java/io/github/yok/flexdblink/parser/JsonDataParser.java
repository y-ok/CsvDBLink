package io.github.yok.flexdblink.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.file.Files;
import java.util.Iterator;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.datatype.DataType;

/**
 * Implementation of {@link DataParser} that reads JSON dataset files from a directory and produces
 * a {@link IDataSet}.
 *
 * <p>
 * Each JSON file should contain an array of objects, where the file name (without extension) is
 * used as the table name, and the object keys are treated as column names.
 * </p>
 */
public class JsonDataParser implements DataParser {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public IDataSet parse(File dir) throws Exception {
        DefaultDataSet dataSet = new DefaultDataSet();

        File[] files = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".json"));
        if (files == null) {
            throw new DataSetException("No JSON files found in directory: " + dir);
        }

        for (File file : files) {
            JsonNode root = mapper.readTree(Files.newBufferedReader(file.toPath()));

            if (!root.isArray() || root.size() == 0) {
                continue;
            }

            // extract column names
            Iterator<String> colNames = root.get(0).fieldNames();
            String[] cols = new String[root.get(0).size()];
            int idx = 0;
            while (colNames.hasNext()) {
                cols[idx++] = colNames.next();
            }

            Column[] dbunitCols = new Column[cols.length];
            for (int i = 0; i < cols.length; i++) {
                dbunitCols[i] = new Column(cols[i], DataType.VARCHAR);
            }
            String tableName =
                    file.getName().substring(0, file.getName().lastIndexOf('.')).toUpperCase();
            DefaultTableMetaData metaData = new DefaultTableMetaData(tableName, dbunitCols);
            DefaultTable table = new DefaultTable(metaData);

            for (JsonNode row : root) {
                Object[] values = new Object[cols.length];
                for (int i = 0; i < cols.length; i++) {
                    JsonNode val = row.get(cols[i]);
                    values[i] = (val != null && !val.isNull()) ? val.asText() : null;
                }
                table.addRow(values);
            }

            dataSet.addTable(table);
        }

        return dataSet;
    }
}
