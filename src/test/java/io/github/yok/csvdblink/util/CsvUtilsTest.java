
package io.github.yok.csvdblink.util;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CsvUtilsTest {

    @Test
    void writeCsvUtf8_正常ケース_ヘッダと複数行が正しく書き込まれること(@TempDir File tmpDir) throws Exception {
        File csvFile = new File(tmpDir, "out.csv");
        String[] headers = {"id", "name", "note"};
        List<List<String>> rows = Arrays.asList(Arrays.asList("1", "Alice", "Hello,World"), // カンマを含む
                Arrays.asList("2", "Bob", "Path\\to\\file"), // バックスラッシュを含む
                Arrays.asList("3", "Carol", " spaced ") // 前後にスペース
        );

        // 実行
        CsvUtils.writeCsvUtf8(csvFile, headers, rows);

        // 内容を読み取って検証
        String content = Files.readString(csvFile.toPath());

        // ヘッダが含まれる
        assertTrue(content.contains("id,name,note"));
        // カンマを含むセルは引用符で囲まれる
        assertTrue(content.contains("\"Hello,World\""));
        // バックスラッシュはエスケープされる
        assertTrue(content.contains("Path\\\\to\\\\file"));
        // スペース付きは引用符で囲まれる
        assertTrue(content.contains("\" spaced \""));
    }

    @Test
    void writeCsvUtf8_異常ケース_ディレクトリ指定でIOExceptionが送出されること(@TempDir File tmpDir) {
        File dirAsFile = tmpDir; // ディレクトリをそのまま渡す
        String[] headers = {"h1"};
        List<List<String>> rows = List.of(List.of("x"));

        assertThrows(IOException.class, () -> CsvUtils.writeCsvUtf8(dirAsFile, headers, rows));
    }

    @Test
    void constructor_正常ケース_privateコンストラクタが存在しインスタンス化できること() throws Exception {
        Constructor<CsvUtils> cons = CsvUtils.class.getDeclaredConstructor();
        assertTrue((cons.getModifiers() & java.lang.reflect.Modifier.PRIVATE) != 0,
                "constructor must be private");
        cons.setAccessible(true);
        Object instance = cons.newInstance();
        assertNotNull(instance);
    }
}
