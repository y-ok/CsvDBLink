package io.github.yok.flexdblink.junit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.flexdblink.config.ConnectionConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import javax.sql.DataSource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mockito;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Slf4j
class LoadDataExtensionTest {

    // テスト対象
    private LoadDataExtension target;

    // ダミーの「テストクラス」
    @LoadData(scenario = {"CSV"}, dbNames = {"bbb"})
    static class DummyTargetTest {
        @LoadData(scenario = {"METHOD_ONLY"}, dbNames = {"bbb"})
        void methodHasScenario() {}

        @LoadData // scenario 未指定（空配列） -> 自動検出分岐を通す
        void methodScenarioAutoDetect() {}
    }

    // テストで使用するパス情報（ターゲットに合わせて両系統を作成）
    private Path classpathRoot; // target/test-classes
    private Path resourcesRoot; // src/test/resources
    private Path dummyClassClassPathDir; // target/test-classes/<pkg>/<ClassName>
    private Path dummyClassResourcesDir; // src/test/resources/<pkg>/<ClassName>

    // 後始末用
    private final List<Path> createdPaths = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        target = new LoadDataExtension();

        classpathRoot = Paths.get("target", "test-classes").toAbsolutePath().normalize();
        resourcesRoot = Paths.get("src", "test", "resources").toAbsolutePath().normalize();

        String pkgPath = DummyTargetTest.class.getPackage().getName().replace('.', '/');
        String clsName = DummyTargetTest.class.getSimpleName();

        dummyClassClassPathDir = classpathRoot.resolve(pkgPath).resolve(clsName);
        dummyClassResourcesDir = resourcesRoot.resolve(pkgPath).resolve(clsName);

        // 両方の系統の「クラスルート」を作成（beforeAll と loadScenario の双方が参照）
        mkDirs(dummyClassClassPathDir);
        mkDirs(dummyClassResourcesDir);

        // クラスレベルのシナリオ "CSV" ディレクトリ（resources 側）
        mkDirs(dummyClassResourcesDir.resolve("CSV").resolve("input").resolve("bbb"));

        // メソッドレベルのシナリオ "METHOD_ONLY" ディレクトリ（resources 側）
        mkDirs(dummyClassResourcesDir.resolve("METHOD_ONLY").resolve("input").resolve("bbb"));

        // 自動検出用：クラスルート直下（classpath 側）にシナリオディレクトリを用意
        mkDirs(dummyClassClassPathDir.resolve("AUTO_SCENARIO")); // beforeTestExecution の自動検出入口
        // resources 側にも対応シナリオ/data を配置
        mkDirs(dummyClassResourcesDir.resolve("AUTO_SCENARIO").resolve("input").resolve("bbb"));

        // application.properties をクラスパス直下に生成（プロファイル無し）
        writeToFile(classpathRoot.resolve("application.properties"),
                "spring.datasource.bbb.url=jdbc:h2:mem:test\n"
                        + "spring.datasource.bbb.username=sa\n"
                        + "spring.datasource.bbb.password=\n"
                        + "spring.datasource.bbb.driver-class-name=org.h2.Driver\n");

        // プロファイル付きの検証用（別テストで利用）
        writeToFile(classpathRoot.resolve("application-profile.properties"),
                "spring.profiles.active=unittest\n");
        writeToFile(classpathRoot.resolve("application-unittest.properties"),
                "spring.datasource.url=jdbc:h2:mem:default\n" + "spring.datasource.username=sa\n");
    }

    @AfterEach
    void tearDown() {
        // TransactionSynchronizationManager にバインドしたものがあれば解除
        try {
            Map<Object, Object> map = TransactionSynchronizationManager.getResourceMap();
            for (Object key : new ArrayList<>(map.keySet())) {
                TransactionSynchronizationManager.unbindResourceIfPossible(key);
            }
        } catch (Exception ignore) {
        }

        // 作成ファイルは残しても良いが、念のため順次削除（存在していれば）
        createdPaths.sort(Comparator.reverseOrder());
        for (Path p : createdPaths) {
            try {
                if (Files.exists(p)) {
                    Files.walk(p).sorted(Comparator.reverseOrder()).forEach(q -> {
                        try {
                            Files.deleteIfExists(q);
                        } catch (IOException ignore) {
                        }
                    });
                }
            } catch (IOException ignore) {
            }
        }
    }

    @Test
    void beforeAll_正常ケース_クラスルートとプロパティが読み込まれること() throws Exception {
        // Arrange
        ExtensionContext ctx = mockContextForClass(DummyTargetTest.class);

        // application.properties は target/test-classes に配置済み

        // Act
        target.beforeAll(ctx);

        // Assert：プライベートフィールド testClassRoot / appProps が設定されていること
        Path testClassRoot = (Path) getField(target, "testClassRoot");
        Properties appProps = (Properties) getField(target, "appProps");
        assertNotNull(testClassRoot);
        assertTrue(testClassRoot.toString().endsWith(DummyTargetTest.class.getSimpleName()));
        assertNotNull(appProps);
        assertEquals("jdbc:h2:mem:test", appProps.getProperty("spring.datasource.bbb.url"));
    }

    @Test
    void beforeAll_異常ケース_クラスリソースが存在しない場合に例外がスローされること() {
        // Arrange: リソースディレクトリを作らない別クラス
        class NoResourceClass {
        }
        ExtensionContext ctx = mockContextForClass(NoResourceClass.class);

        // Act & Assert
        Exception ex = assertThrows(IllegalStateException.class, () -> target.beforeAll(ctx));
        assertTrue(ex.getMessage().contains("was not found"));
    }

    // --------------------------------------------------------------------------------------------
    // beforeTestExecution（クラスアノテーション／メソッドアノテーション両方）
    // --------------------------------------------------------------------------------------------

    @Test
    void beforeTestExecution_正常ケース_クラスアノテーションのシナリオ存在時にロードが行われること() throws Exception {
        // Arrange: Spring管理の DataSource をバインド（中身はダミー）
        DataSource ds = dummyDataSource();
        TransactionSynchronizationManager.bindResource(ds, new Object());

        // Context: クラスレベルアノテーション（@LoadData(scenario={"CSV"}, dbNames={"bbb"})）
        ExtensionContext ctx = mockContextForClass(DummyTargetTest.class);

        // Act & Assert: 例外が発生しないこと（空ディレクトリのため DataLoader は即戻る想定）
        target.beforeAll(ctx);
        assertDoesNotThrow(() -> target.beforeTestExecution(ctx));
    }

    @Test
    void beforeTestExecution_正常ケース_メソッドアノテーションのシナリオ不存在時にスキップされること() throws Exception {
        // Arrange
        DataSource ds = dummyDataSource();
        TransactionSynchronizationManager.bindResource(ds, new Object());

        // 対象メソッド（@LoadData(scenario={"METHOD_ONLY"})）
        Method dummyMethod = DummyTargetTest.class.getDeclaredMethod("methodHasScenario");

        // ExtensionContext をモックし、ジェネリクス問題を避けるため doReturn(..).when(..) を使用
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(DummyTargetTest.class).when(ctx).getRequiredTestClass();
        doReturn(Optional.of(dummyMethod)).when(ctx).getTestMethod();

        // クラスパス側（target/test-classes）には "METHOD_ONLY" を作成していないため、
        // beforeTestExecution 内の分岐で「not found → スキップ」となるのが期待動作
        target.beforeAll(ctx);

        // Act & Assert
        assertDoesNotThrow(() -> target.beforeTestExecution(ctx));
    }

    @Test
    void beforeTestExecution_異常ケース_Spring管理DataSource未バインドでエラーとなること() throws Exception {
        // Arrange: DataSource をバインドしない
        ExtensionContext ctx = mockContextForClass(DummyTargetTest.class);

        // クラスレベルのシナリオ @LoadData(scenario={"CSV"}, dbNames={"bbb"}) を発火させるため、
        // 「クラスパス側」のクラスルート直下に CSV ディレクトリを作成しておく
        // これがないとシナリオ未検出でロードがスキップされ、例外が出ない
        Files.createDirectories(dummyClassClassPathDir.resolve("CSV"));

        // Act
        target.beforeAll(ctx);
        Exception ex =
                assertThrows(IllegalStateException.class, () -> target.beforeTestExecution(ctx));

        // Assert
        assertTrue(ex.getMessage().contains("No Spring-managed DataSource"));
    }

    @Test
    void wrapConnectionNoClose_正常ケース_close呼び出しが無視されること() throws Exception {
        // Arrange: Connection モックを生成
        Connection original = mock(Connection.class);
        when(original.getAutoCommit()).thenReturn(true); // 委譲確認用

        // private メソッド呼び出し
        Method m = LoadDataExtension.class.getDeclaredMethod("wrapConnectionNoClose",
                Connection.class);
        m.setAccessible(true);
        Connection proxy = (Connection) m.invoke(target, original);

        // Act: close() は無視されるべき
        proxy.close();

        // 他のメソッドは委譲されるべき
        boolean autoCommit = proxy.getAutoCommit();

        // Assert
        verify(original, never()).close();
        verify(original, times(1)).getAutoCommit();
        assertTrue(autoCommit);
    }

    @Test
    void maybeGetSpringManagedDataSource_正常ケース_バインド済みのDataSourceが返却されること() throws Exception {
        // Arrange
        DataSource ds = dummyDataSource();
        TransactionSynchronizationManager.bindResource(ds, new Object());

        Method m = LoadDataExtension.class.getDeclaredMethod("maybeGetSpringManagedDataSource",
                ExtensionContext.class, String.class);
        m.setAccessible(true);

        // Act
        @SuppressWarnings("unchecked")
        Optional<DataSource> opt =
                (Optional<DataSource>) m.invoke(target, mock(ExtensionContext.class), "bbb");

        // Assert
        assertTrue(opt.isPresent());
        assertSame(ds, opt.get());
    }

    @Test
    void maybeGetSpringManagedDataSource_異常ケース_未バインドの場合は空で返ること() throws Exception {
        Method m = LoadDataExtension.class.getDeclaredMethod("maybeGetSpringManagedDataSource",
                ExtensionContext.class, String.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        Optional<DataSource> opt =
                (Optional<DataSource>) m.invoke(target, mock(ExtensionContext.class), "bbb");

        assertTrue(opt.isEmpty());
    }

    @Test
    void buildEntryFromProps_正常ケース_db指定ありで接続情報が解決されること() throws Exception {
        // Arrange: appProps を上書き
        Properties p = new Properties();
        p.setProperty("spring.datasource.bbb.url", "jdbc:h2:mem:x");
        p.setProperty("spring.datasource.bbb.username", "sa");
        p.setProperty("spring.datasource.bbb.password", "");
        p.setProperty("spring.datasource.bbb.driver-class-name", "org.h2.Driver");
        setField(target, "appProps", p);

        Method m = LoadDataExtension.class.getDeclaredMethod("buildEntryFromProps", String.class);
        m.setAccessible(true);

        // Act
        ConnectionConfig.Entry entry = (ConnectionConfig.Entry) m.invoke(target, "bbb");

        // Assert
        assertEquals("bbb", entry.getId());
        assertEquals("jdbc:h2:mem:x", entry.getUrl());
        assertEquals("sa", entry.getUser());
        assertEquals("org.h2.Driver", entry.getDriverClass());
    }

    @Test
    void buildEntryFromProps_正常ケース_db指定なしでデフォルト接続情報が解決されること() throws Exception {
        // Arrange: spring.datasource.url / username だけを用意（suffix マッチ＋スコアリング分岐）
        Properties p = new Properties();
        p.setProperty("spring.datasource.url", "jdbc:h2:mem:default");
        p.setProperty("spring.datasource.username", "sa");
        setField(target, "appProps", p);

        Method m = LoadDataExtension.class.getDeclaredMethod("buildEntryFromProps", String.class);
        m.setAccessible(true);

        // Act: 単一 null 引数は new Object[]{null} で渡す（null そのものは「引数配列なし」と解釈される）
        ConnectionConfig.Entry entry =
                (ConnectionConfig.Entry) m.invoke(target, new Object[] {null});

        // Assert
        assertEquals("default", entry.getId());
        assertEquals("jdbc:h2:mem:default", entry.getUrl());
        assertEquals("sa", entry.getUser());
    }

    @Test
    void resolveDatasetDir_正常ケース_シナリオありの完全パスが構築されること() throws Exception {
        Method m = LoadDataExtension.class.getDeclaredMethod("resolveDatasetDir", Path.class,
                String.class, String.class);
        m.setAccessible(true);

        Path resolved = (Path) m.invoke(target, dummyClassResourcesDir, "CSV", "bbb");
        assertTrue(resolved.toString()
                .endsWith(DummyTargetTest.class.getSimpleName() + "/CSV/input/bbb"));
    }

    @Test
    void resolveDatasetDir_正常ケース_シナリオなしの完全パスが構築されること() throws Exception {
        Method m = LoadDataExtension.class.getDeclaredMethod("resolveDatasetDir", Path.class,
                String.class, String.class);
        m.setAccessible(true);

        Path resolved = (Path) m.invoke(target, dummyClassResourcesDir, "", "bbb");
        assertTrue(
                resolved.toString().endsWith(DummyTargetTest.class.getSimpleName() + "/input/bbb"));
    }

    @Test
    void resolveBaseForDbDetection_正常ケース_シナリオありのベースパスが構築されること() throws Exception {
        Method m = LoadDataExtension.class.getDeclaredMethod("resolveBaseForDbDetection",
                Path.class, String.class);
        m.setAccessible(true);

        Path base = (Path) m.invoke(target, dummyClassResourcesDir, "CSV");
        assertTrue(base.toString().endsWith(DummyTargetTest.class.getSimpleName() + "/CSV/input"));
    }

    @Test
    void resolveBaseForDbDetection_正常ケース_シナリオなしのベースパスが構築されること() throws Exception {
        Method m = LoadDataExtension.class.getDeclaredMethod("resolveBaseForDbDetection",
                Path.class, String.class);
        m.setAccessible(true);

        Path base = (Path) m.invoke(target, dummyClassResourcesDir, null);
        assertTrue(base.toString().endsWith(DummyTargetTest.class.getSimpleName() + "/input"));
    }

    @Test
    void detectDbNames_正常ケース_input配下のDBフォルダが検出されること() throws Exception {
        // Arrange: AUTO_SCENARIO/input/bbb を resources 側にも作成済み
        Method m = LoadDataExtension.class.getDeclaredMethod("detectDbNames", Path.class);
        m.setAccessible(true);

        Path base = dummyClassResourcesDir.resolve("AUTO_SCENARIO").resolve("input");

        // Act
        @SuppressWarnings("unchecked")
        List<String> names = (List<String>) m.invoke(target, base);

        // Assert
        assertTrue(names.contains("bbb"));
    }

    private ExtensionContext mockContextForClass(Class<?> clazz) {
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(clazz).when(ctx).getRequiredTestClass();
        doReturn(Optional.empty()).when(ctx).getTestMethod();
        return ctx;
    }

    private void mkDirs(Path p) {
        try {
            Files.createDirectories(p);
            createdPaths.add(p);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeToFile(Path p, String content) {
        try {
            Files.createDirectories(p.getParent());
            Files.write(p, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
            createdPaths.add(p);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private DataSource dummyDataSource() throws Exception {
        Connection conn = mock(Connection.class, Mockito.withSettings().name("DummyConnection"));
        when(conn.getWarnings()).thenReturn(new SQLWarning());
        when(conn.getMetaData()).thenReturn(mock(DatabaseMetaData.class));

        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);
        return ds;
    }

    // リフレクション用
    @SneakyThrows
    private static Object getField(Object target, String name) {
        var f = LoadDataExtension.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }

    @SneakyThrows
    private static void setField(Object target, String name, Object value) {
        var f = LoadDataExtension.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }
}
