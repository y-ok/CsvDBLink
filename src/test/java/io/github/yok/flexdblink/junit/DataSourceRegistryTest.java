package io.github.yok.flexdblink.junit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DataSourceRegistryTest {

    @AfterEach
    void cleanup() {
        // 各テストの副作用を消す
        DataSourceRegistry.clear();
    }

    @Test
    void register_正常ケース_登録後findで取得できること() {
        DataSource ds = Mockito.mock(DataSource.class);
        DataSourceRegistry.register("aaa", ds);

        Optional<DataSource> found = DataSourceRegistry.find("aaa");
        assertTrue(found.isPresent());
        assertSame(ds, found.get());
    }

    @Test
    void register_異常ケース_dbNameがnullで例外となること() {
        DataSource ds = Mockito.mock(DataSource.class);
        assertThrows(NullPointerException.class, () -> DataSourceRegistry.register(null, ds));
    }

    @Test
    void register_異常ケース_dataSourceがnullで例外となること() {
        assertThrows(NullPointerException.class, () -> DataSourceRegistry.register("aaa", null));
    }

    @Test
    void registerAll_正常ケース_複数登録できること() {
        DataSource ds1 = Mockito.mock(DataSource.class);
        DataSource ds2 = Mockito.mock(DataSource.class);

        Map<String, DataSource> map = new HashMap<>();
        map.put("a", ds1);
        map.put("b", ds2);

        DataSourceRegistry.registerAll(map);

        assertTrue(DataSourceRegistry.find("a").isPresent());
        assertTrue(DataSourceRegistry.find("b").isPresent());
    }

    @Test
    void registerAll_異常ケース_null指定で例外となること() {
        assertThrows(NullPointerException.class, () -> DataSourceRegistry.registerAll(null));
    }

    @Test
    void find_正常ケース_登録済みはOptionalで返ること() {
        DataSource ds = Mockito.mock(DataSource.class);
        DataSourceRegistry.register("xxx", ds);

        Optional<DataSource> got = DataSourceRegistry.find("xxx");
        assertTrue(got.isPresent());
        assertSame(ds, got.get());
    }

    @Test
    void find_正常ケース_未登録はemptyが返ること() {
        Optional<DataSource> got = DataSourceRegistry.find("zzz");
        assertTrue(got.isEmpty());
    }

    @Test
    void find_正常ケース_nullキーはemptyが返ること() {
        Optional<DataSource> got = DataSourceRegistry.find(null);
        assertTrue(got.isEmpty());
    }

    @Test
    void snapshot_正常ケース_空の場合は空Mapが返ること() {
        Map<String, DataSource> snap = DataSourceRegistry.snapshot();
        assertTrue(snap.isEmpty());
    }

    @Test
    void snapshot_正常ケース_登録済みは含まれること_イミュータブルであること() {
        DataSource ds = Mockito.mock(DataSource.class);
        DataSourceRegistry.register("id1", ds);

        Map<String, DataSource> snap = DataSourceRegistry.snapshot();
        assertEquals(1, snap.size());
        assertSame(ds, snap.get("id1"));

        // ImmutableMapなのでUnsupportedOperationExceptionになることを確認
        assertThrows(UnsupportedOperationException.class, () -> snap.put("x", ds));
    }

    @Test
    void unregister_正常ケース_登録削除できること() {
        DataSource ds = Mockito.mock(DataSource.class);
        DataSourceRegistry.register("id1", ds);

        assertTrue(DataSourceRegistry.find("id1").isPresent());

        DataSourceRegistry.unregister("id1");
        assertTrue(DataSourceRegistry.find("id1").isEmpty());
    }

    @Test
    void unregister_正常ケース_dbNameがnullなら何も起きないこと() {
        // 例外が発生しないこと
        assertDoesNotThrow(() -> DataSourceRegistry.unregister(null));
    }

    @Test
    void clear_正常ケース_全件削除されること() {
        DataSource ds = Mockito.mock(DataSource.class);
        DataSourceRegistry.register("id1", ds);
        assertTrue(DataSourceRegistry.find("id1").isPresent());

        DataSourceRegistry.clear();
        assertTrue(DataSourceRegistry.find("id1").isEmpty());
    }
}
