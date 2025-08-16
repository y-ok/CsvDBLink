package io.github.yok.csvdblink.junit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.collect.ImmutableList;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.logging.Logger;
import javax.sql.DataSource;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.transaction.support.TransactionSynchronizationManager;

class LoadCsvDataExtensionTest {

    private DataSource boundDsForCleanup;
    private boolean tcclOverridden;
    private ClassLoader prevTccl;

    @AfterEach
    void cleanup() {
        if (boundDsForCleanup != null) {
            TransactionSynchronizationManager.unbindResource(boundDsForCleanup);
            boundDsForCleanup = null;
        }
        if (tcclOverridden) {
            Thread.currentThread().setContextClassLoader(prevTccl);
            tcclOverridden = false;
            prevTccl = null;
        }
    }

    @SneakyThrows
    private static void setPrivateField(Object target, String fieldName, Object value) {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private static <T> T invokePrivate(Object target, String methodName, Class<?>[] types,
            Object... args) {
        Method m = target.getClass().getDeclaredMethod(methodName, types);
        m.setAccessible(true);
        Object ret = m.invoke(target, args);
        return (T) ret;
    }

    @SneakyThrows
    private static Object getPrivateField(Object target, String fieldName) {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        return f.get(target);
    }

    static class DummyClass_MethodAnn_DB指定_Spring_Participate {
        @LoadCsvData(scenario = {"scenario1"}, dbNames = {"bbb"})
        void dummy() {}
    }

    static class DummyClass_MethodAnn_DB指定_NonSpring_Recording {
        @LoadCsvData(scenario = {"s_ns"}, dbNames = {"db2"})
        void dummy() {}
    }

    static class DummyClass_MethodAnn_DBspecified_NonSpring_Recording {
        @LoadCsvData(scenario = {"s_ns"}, dbNames = {"db2"})
        void dummy() {}
    }

    static class DummyForResource {
    }

    static class DummyClass_MethodAnn_ScenarioExists_NoDbDir {
        @LoadCsvData(scenario = {"m_ok"})
        void dummy() {}
    }

    @LoadCsvData(scenario = {"missing"})
    static class DummyClass_ClassAnn_Missing {
    }

    static class DummyClass_MethodAnn_Missing {
        @LoadCsvData(scenario = {"m1"})
        void dummy() {}
    }

    static class DummyClass_MethodAnn_DbSpecified {
        @LoadCsvData(scenario = {"m2"}, dbNames = {"dbx"})
        void dummy() {}
    }

    static class DummyClass_MethodAnn_None {
        void dummy() {}
    }

    static class DummyClass_MethodAnn_ScenarioNotFound {
        @LoadCsvData(scenario = {"noDir"})
        void dummy() {}
    }

    @LoadCsvData(scenario = {"c1"})
    static class DummyClass_ClassAnn_Exists {
    }

    static class DummyClass_MethodAnn_SpringBranch {
        @LoadCsvData(scenario = {"s_spring"}, dbNames = {"db1"})
        void dummy() {}
    }

    static class DummyClass_MethodAnn_NonSpringBranch {
        @LoadCsvData(scenario = {"s_non"}, dbNames = {"db2"})
        void dummy() {}
    }

    @Test
    void wrapConnectionNoClose_正常ケース_closeが無視されること() throws Exception {
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        Connection original = mock(Connection.class);
        Connection wrapped = invokePrivate(ext, "wrapConnectionNoClose",
                new Class<?>[] {Connection.class}, original);
        wrapped.close();
        verify(original, never()).close();
    }

    @Test
    void wrapConnectionNoClose_正常ケース_他メソッドは委譲されること() throws Exception {
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        Connection original = mock(Connection.class);
        Connection wrapped = invokePrivate(ext, "wrapConnectionNoClose",
                new Class<?>[] {Connection.class}, original);
        wrapped.setAutoCommit(false);
        verify(original, times(1)).setAutoCommit(false);
    }

    @Test
    void resolvePropertyForDb_正常ケース_DB無し優先_標準springDatasourceが選択されること() throws Exception {
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        Properties p = new Properties();
        p.setProperty("spring.datasource.url", "jdbc:h2:mem:test1");
        p.setProperty("x.y.z.datasource.url", "jdbc:h2:mem:other");
        String v = invokePrivate(ext, "resolvePropertyForDb",
                new Class<?>[] {Properties.class, String.class, String.class}, p, null, "url");
        assertEquals("jdbc:h2:mem:test1", v);
    }

    @Test
    void resolvePropertyForDb_正常ケース_DB名一致_最も近いキーが選択されること() throws Exception {
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        Properties p = new Properties();
        p.setProperty("spring.datasource.operator.bbb.url", "jdbc:h2:mem:bbb-close");
        p.setProperty("spring.datasource.operator.url", "jdbc:h2:mem:operator");
        p.setProperty("foo.bar.operator.bbb.baz.datasource.url", "jdbc:h2:mem:far");
        String v = invokePrivate(ext, "resolvePropertyForDb",
                new Class<?>[] {Properties.class, String.class, String.class}, p, "operator.bbb",
                "url");
        assertEquals("jdbc:h2:mem:bbb-close", v);
    }

    @Test
    void resolvePropertyForDb_正常ケース_距離ペナルティ_遠いキーより近いキーが選ばれること() throws Exception {
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        Properties p = new Properties();
        p.setProperty("a.b.operator.bbb.datasource.url", "jdbc:h2:mem:near");
        p.setProperty("a.operator.bbb.x.y.datasource.url", "jdbc:h2:mem:far");
        String v = invokePrivate(ext, "resolvePropertyForDb",
                new Class<?>[] {Properties.class, String.class, String.class}, p, "operator.bbb",
                "url");
        assertEquals("jdbc:h2:mem:near", v);
    }

    @Test
    void resolvePropertyForDb_異常ケース_該当無し_nullが返ること() throws Exception {
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        Properties p = new Properties();
        p.setProperty("spring.datasource.url", "jdbc:h2:mem:x");
        String v = invokePrivate(ext, "resolvePropertyForDb",
                new Class<?>[] {Properties.class, String.class, String.class}, p, "no.hit",
                "username");
        assertNull(v);
    }

    @Test
    void listDirectories_正常ケース_サブディレクトリのみ抽出されソートされること(@TempDir Path tmp) throws Exception {
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        Files.createDirectory(tmp.resolve("a"));
        Files.createDirectory(tmp.resolve("c"));
        Files.createDirectory(tmp.resolve("b"));
        Files.writeString(tmp.resolve("x.txt"), "ignored");
        var list = invokePrivate(ext, "listDirectories", new Class<?>[] {Path.class}, tmp);
        assertEquals(ImmutableList.of("a", "b", "c"), list);
    }

    @Test
    void resolveScenarios_正常ケース_アノテーション指定時_指定リストが返ること() throws Exception {
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        LoadCsvData ann = new LoadCsvData() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return LoadCsvData.class;
            }

            @Override
            public String[] scenario() {
                return new String[] {"s1", "s2"};
            }

            @Override
            public String[] dbNames() {
                return new String[0];
            }
        };
        var list = invokePrivate(ext, "resolveScenarios", new Class<?>[] {LoadCsvData.class}, ann);
        assertEquals(ImmutableList.of("s1", "s2"), list);
    }

    @Test
    void maybeGetSpringManagedDataSource_正常ケース_トランザクションにバインド済み_そのDataSourceが返ること()
            throws Exception {
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        ExtensionContext ctx = mock(ExtensionContext.class);
        DataSource ds = mock(DataSource.class);
        TransactionSynchronizationManager.bindResource(ds, new Object());
        boundDsForCleanup = ds;
        Optional<DataSource> got = invokePrivate(ext, "maybeGetSpringManagedDataSource",
                new Class<?>[] {ExtensionContext.class, String.class}, ctx, "operator.bbb");
        assertTrue(got.isPresent());
    }

    @Test
    void resolveTestClassRoot_正常ケース_一時クラスローダー経由で解決できること(@TempDir Path tmp) throws Exception {
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        Class<?> target = DummyForResource.class;
        String pkgPath = target.getPackage().getName().replace('.', '/');
        Path base = tmp;
        Path dir = base.resolve(pkgPath).resolve(target.getSimpleName());
        Files.createDirectories(dir);
        prevTccl = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {base.toUri().toURL()}, prevTccl);
        Thread.currentThread().setContextClassLoader(cl);
        tcclOverridden = true;
        Path root =
                invokePrivate(ext, "resolveTestClassRoot", new Class<?>[] {Class.class}, target);
        cl.close();
        assertTrue(Files.isSameFile(dir, root));
    }

    @Test
    void buildEntryFromProps_正常ケース_プロパティからEntryが構築されること() throws Exception {
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        Properties p = new Properties();
        p.setProperty("spring.datasource.operator.bbb.url", "jdbc:h2:mem:x");
        p.setProperty("spring.datasource.operator.bbb.username", "u");
        p.setProperty("spring.datasource.operator.bbb.password", "p");
        p.setProperty("spring.datasource.operator.bbb.driver-class-name", "org.h2.Driver");
        setPrivateField(ext, "appProps", p);
        Object entry = invokePrivate(ext, "buildEntryFromProps", new Class<?>[] {String.class},
                "operator.bbb");
        Class<?> entryClz = entry.getClass();
        Method getId = entryClz.getMethod("getId");
        Method getUrl = entryClz.getMethod("getUrl");
        Method getUser = entryClz.getMethod("getUser");
        assertEquals("operator.bbb", getId.invoke(entry));
        assertEquals("jdbc:h2:mem:x", getUrl.invoke(entry));
        assertEquals("u", getUser.invoke(entry));
    }

    @Test
    void buildEntryFromProps_異常ケース_必須欠落_例外がスローされること() throws Exception {
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        Properties p = new Properties();
        p.setProperty("spring.datasource.url", "jdbc:h2:mem:missing-user");
        setPrivateField(ext, "appProps", p);
        InvocationTargetException ex =
                assertThrows(InvocationTargetException.class, () -> invokePrivate(ext,
                        "buildEntryFromProps", new Class<?>[] {String.class}, (Object) null));
        assertTrue(ex.getCause() instanceof IllegalStateException);
        assertTrue(ex.getCause().getMessage().contains("Missing connection info"));
    }

    @Test
    void beforeTestExecution_正常ケース_クラスアノテーションシナリオ無し_例外無く終了すること(@TempDir Path tmp) throws Exception {
        Path base = tmp;
        Files.writeString(base.resolve("application.properties"),
                "spring.datasource.url=jdbc:h2:mem:x\nspring.datasource.username=u\n");
        Class<?> testClass = DummyClass_ClassAnn_Missing.class;
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        Path classRoot = base.resolve(pkgPath).resolve(testClass.getSimpleName());
        Files.createDirectories(classRoot);
        prevTccl = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {base.toUri().toURL()}, prevTccl);
        Thread.currentThread().setContextClassLoader(cl);
        tcclOverridden = true;
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(testClass).when(ctx).getRequiredTestClass();
        doReturn(Optional.empty()).when(ctx).getTestMethod();
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        ext.beforeAll(ctx);
        assertDoesNotThrow(() -> ext.beforeTestExecution(ctx));
        cl.close();
    }

    @Test
    void beforeTestExecution_正常ケース_メソッドアノテーションシナリオ無し_例外無く終了すること(@TempDir Path tmp)
            throws Exception {
        Path base = tmp;
        Files.writeString(base.resolve("application.properties"),
                "spring.datasource.url=jdbc:h2:mem:x\nspring.datasource.username=u\n");
        Class<?> testClass = DummyClass_MethodAnn_Missing.class;
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        Path classRoot = base.resolve(pkgPath).resolve(testClass.getSimpleName());
        Files.createDirectories(classRoot);
        prevTccl = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {base.toUri().toURL()}, prevTccl);
        Thread.currentThread().setContextClassLoader(cl);
        tcclOverridden = true;
        Method m = testClass.getDeclaredMethod("dummy");
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(testClass).when(ctx).getRequiredTestClass();
        doReturn(Optional.of(m)).when(ctx).getTestMethod();
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        ext.beforeAll(ctx);
        assertDoesNotThrow(() -> ext.beforeTestExecution(ctx));
        cl.close();
    }

    @Test
    void beforeTestExecution_正常ケース_クラスアノテーションシナリオ有りDB無し_例外無く終了すること(@TempDir Path tmp)
            throws Exception {
        Path base = tmp;
        Files.writeString(base.resolve("application.properties"),
                "spring.datasource.url=jdbc:h2:mem:x\nspring.datasource.username=u\n");
        Class<?> testClass = DummyClass_ClassAnn_Exists.class;
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        Path classRoot = base.resolve(pkgPath).resolve(testClass.getSimpleName());
        Files.createDirectories(classRoot.resolve("c1"));
        prevTccl = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {base.toUri().toURL()}, prevTccl);
        Thread.currentThread().setContextClassLoader(cl);
        tcclOverridden = true;
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(testClass).when(ctx).getRequiredTestClass();
        doReturn(Optional.empty()).when(ctx).getTestMethod();
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        ext.beforeAll(ctx);
        assertDoesNotThrow(() -> ext.beforeTestExecution(ctx));
        cl.close();
    }

    @Test
    void beforeTestExecution_異常ケース_メソッドアノテーションDB指定_RuntimeExceptionが送出されること(@TempDir Path tmp)
            throws Exception {

        // application.properties を作成
        Files.writeString(tmp.resolve("application.properties"),
                "spring.datasource.url=jdbc:h2:mem:x\nspring.datasource.username=u\n");

        Class<?> testClass = DummyClass_MethodAnn_DbSpecified.class;
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        Path classRoot = tmp.resolve(pkgPath).resolve(testClass.getSimpleName());

        // シナリオフォルダを作成
        Files.createDirectories(classRoot.resolve("m2"));

        Method m = testClass.getDeclaredMethod("dummy");
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(testClass).when(ctx).getRequiredTestClass();
        doReturn(Optional.of(m)).when(ctx).getTestMethod();

        LoadCsvDataExtension ext = new LoadCsvDataExtension();

        // ★ tmp をクラスパスに載せる（application.properties を見つけられるようにする）
        prevTccl = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {tmp.toUri().toURL()}, prevTccl);
        Thread.currentThread().setContextClassLoader(cl);
        tcclOverridden = true;

        ext.beforeAll(ctx);

        assertThrows(RuntimeException.class, () -> ext.beforeTestExecution(ctx));

        cl.close();
    }

    @Test
    void beforeTestExecution_正常ケース_メソッド注釈無し_何もせず終了すること(@TempDir Path tmp) throws Exception {
        Path base = tmp;
        Files.writeString(base.resolve("application.properties"),
                "spring.datasource.url=jdbc:h2:mem:x\nspring.datasource.username=u\n");
        Class<?> testClass = DummyClass_MethodAnn_None.class;
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        Path classRoot = base.resolve(pkgPath).resolve(testClass.getSimpleName());
        Files.createDirectories(classRoot);
        prevTccl = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {base.toUri().toURL()}, prevTccl);
        Thread.currentThread().setContextClassLoader(cl);
        tcclOverridden = true;
        Method m = testClass.getDeclaredMethod("dummy");
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(testClass).when(ctx).getRequiredTestClass();
        doReturn(Optional.of(m)).when(ctx).getTestMethod();
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        ext.beforeAll(ctx);
        assertDoesNotThrow(() -> ext.beforeTestExecution(ctx));
        cl.close();
    }

    @Test
    void beforeTestExecution_正常ケース_メソッドシナリオ未存在_例外無く終了すること(@TempDir Path tmp) throws Exception {
        Path base = tmp;
        Files.writeString(base.resolve("application.properties"),
                "spring.datasource.url=jdbc:h2:mem:x\nspring.datasource.username=u\n");
        Class<?> testClass = DummyClass_MethodAnn_ScenarioNotFound.class;
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        Path classRoot = base.resolve(pkgPath).resolve(testClass.getSimpleName());
        Files.createDirectories(classRoot); // シナリオ(noDir)ディレクトリは作らない
        prevTccl = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {base.toUri().toURL()}, prevTccl);
        Thread.currentThread().setContextClassLoader(cl);
        tcclOverridden = true;
        Method m = testClass.getDeclaredMethod("dummy");
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(testClass).when(ctx).getRequiredTestClass();
        doReturn(Optional.of(m)).when(ctx).getTestMethod();
        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        ext.beforeAll(ctx);
        assertDoesNotThrow(() -> ext.beforeTestExecution(ctx));
        cl.close();
    }

    @Test
    void beforeTestExecution_正常ケース_メソッドシナリオ存在DB未指定_ロード実行経路を通って正常終了すること(@TempDir Path tmp)
            throws Exception {
        Path base = tmp;
        Files.writeString(base.resolve("application.properties"),
                "spring.datasource.url=jdbc:h2:mem:x\nspring.datasource.username=u\n");
        Class<?> testClass = DummyClass_MethodAnn_ScenarioExists_NoDbDir.class;
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        Path classRoot = base.resolve(pkgPath).resolve(testClass.getSimpleName());
        Files.createDirectories(classRoot.resolve("m_ok")); // シナリオディレクトリのみ作成（DBサブディレクトリは作らない）

        prevTccl = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {base.toUri().toURL()}, prevTccl);
        Thread.currentThread().setContextClassLoader(cl);
        tcclOverridden = true;

        Method m = testClass.getDeclaredMethod("dummy");
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(testClass).when(ctx).getRequiredTestClass();
        doReturn(Optional.of(m)).when(ctx).getTestMethod();

        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        ext.beforeAll(ctx);
        assertDoesNotThrow(() -> ext.beforeTestExecution(ctx));

        cl.close();
    }

    static final class StubDriver implements Driver {
        static Connection CONN;

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith("jdbc:stub:");
        }

        @Override
        public Connection connect(String url, Properties info) {
            return acceptsURL(url) ? (CONN != null ? CONN : null) : null;
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }
    }

    static final class RecordingMap extends LinkedHashMap<String, Connection> {
        boolean called;

        @Override
        public Connection computeIfAbsent(String key,
                Function<? super String, ? extends Connection> mappingFunction) {
            called = true;
            Connection c = mock(Connection.class);
            super.put(key, c);
            return c;
        }

        boolean wasCalled() {
            return called;
        }
    }

    @Test
    void beforeTestExecution_異常ケース_メソッドDB指定_ロード実行で例外が送出されること(@TempDir Path tmp) throws Exception {
        Path base = tmp;
        Files.writeString(base.resolve("application.properties"),
                "spring.datasource.db1.url=jdbc:stub:db1\nspring.datasource.db1.username=u\n");

        Class<?> testClass = DummyClass_MethodAnn_SpringBranch.class;
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        Path classRoot = base.resolve(pkgPath).resolve(testClass.getSimpleName());
        Files.createDirectories(classRoot.resolve("s_spring").resolve("db1"));

        prevTccl = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {base.toUri().toURL()}, prevTccl);
        Thread.currentThread().setContextClassLoader(cl);
        tcclOverridden = true;

        Method m = testClass.getDeclaredMethod("dummy");
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(testClass).when(ctx).getRequiredTestClass();
        doReturn(Optional.of(m)).when(ctx).getTestMethod();

        DataSource ds = mock(DataSource.class);
        ConnectionHolder holder = new ConnectionHolder(mock(Connection.class));
        TransactionSynchronizationManager.bindResource(ds, holder);
        boundDsForCleanup = ds;

        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        ext.beforeAll(ctx);

        RuntimeException ex =
                assertThrows(RuntimeException.class, () -> ext.beforeTestExecution(ctx));
        assertNotNull(ex.getCause());

        cl.close();
    }

    static final class TestStubDriver implements Driver {
        private final Connection conn;

        TestStubDriver() {
            this.conn = mock(Connection.class);
            DatabaseMetaData meta = mock(DatabaseMetaData.class);
            try {
                when(conn.getMetaData()).thenReturn(meta);
                when(meta.getDatabaseProductName()).thenReturn("StubDB");
                when(meta.getUserName()).thenReturn("U");
                when(meta.getURL()).thenReturn("jdbc:stub:db2");
                when(meta.getIdentifierQuoteString()).thenReturn("\"");
                when(meta.supportsMixedCaseIdentifiers()).thenReturn(false);
                when(meta.storesLowerCaseIdentifiers()).thenReturn(true);
                when(meta.storesUpperCaseIdentifiers()).thenReturn(false);
            } catch (Exception ignore) {
            }
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith("jdbc:stub:");
        }

        @Override
        public Connection connect(String url, Properties info) {
            return acceptsURL(url) ? conn : null;
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }
    }

    static class DummyClass_ProfileLoad_Success {
        @LoadCsvData(scenario = {"s"})
        void dummy() {}
    }

    static class DummyClass_ProfileLoad_Missing {
        @LoadCsvData(scenario = {"s"})
        void dummy() {}
    }

    @Test
    void beforeAll_正常ケース_プロファイル有り_プロファイルプロパティが読み込まれること(@TempDir Path tmp) throws Exception {
        Path base = tmp;
        Files.writeString(base.resolve("application.properties"),
                "spring.profiles.active=dev\nfoo=base\n");
        Files.writeString(base.resolve("application-dev.properties"), "foo=profile\n");

        Class<?> testClass = DummyClass_ProfileLoad_Success.class;
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        Path classRoot = base.resolve(pkgPath).resolve(testClass.getSimpleName());
        Files.createDirectories(classRoot);

        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {base.toUri().toURL()}, prev);
        Thread.currentThread().setContextClassLoader(cl);

        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(testClass).when(ctx).getRequiredTestClass();

        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        ext.beforeAll(ctx);

        Properties props = (Properties) getPrivateField(ext, "appProps");
        assertEquals("dev", props.getProperty("spring.profiles.active"));
        assertEquals("profile", props.getProperty("foo"));

        Thread.currentThread().setContextClassLoader(prev);
        cl.close();
    }

    @Test
    void beforeAll_異常ケース_プロファイル有り_プロファイルファイル不在_例外が送出されること(@TempDir Path tmp) throws Exception {
        Path base = tmp;
        Files.writeString(base.resolve("application.properties"), "spring.profiles.active=dev\n");

        Class<?> testClass = DummyClass_ProfileLoad_Missing.class;
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        Path classRoot = base.resolve(pkgPath).resolve(testClass.getSimpleName());
        Files.createDirectories(classRoot);

        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {base.toUri().toURL()}, prev);
        Thread.currentThread().setContextClassLoader(cl);

        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(testClass).when(ctx).getRequiredTestClass();

        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> ext.beforeAll(ctx));
        assertTrue(ex.getMessage().contains("application-dev.properties"));

        Thread.currentThread().setContextClassLoader(prev);
        cl.close();
    }

    static class DummyClass_MethodAnn_SpringBranch_PathGetters {
        @LoadCsvData(scenario = {"s_path"}, dbNames = {"dbp"})
        void dummy() {}
    }

    static class DummyClass_MethodAnn_SpringBranch_UserUpper {
        @LoadCsvData(scenario = {"s_upper"}, dbNames = {"dbu"})
        void dummy() {}
    }

    @Test
    public void beforeTestExecution_異常ケース_メソッドDB指定_pathsConfigのgetLoad_getDataPathが評価される経路_例外がスローされること(
            @TempDir Path tmp) throws Exception {

        // --- クラスパスに application.properties を用意（dbp の URL/ユーザーを設定） ---
        Files.writeString(tmp.resolve("application.properties"),
                "spring.datasource.dbp.url=jdbc:stub:dbp\n"
                        + "spring.datasource.dbp.username=userp\n");

        // --- テスト用のクラス配下にシナリオ/DB ディレクトリを用意 ---
        Class<?> testClass = DummyClass_MethodAnn_SpringBranch_PathGetters.class;
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        Path classRoot = tmp.resolve(pkgPath).resolve(testClass.getSimpleName());
        Files.createDirectories(classRoot.resolve("s_path").resolve("dbp"));

        // --- TCCL を tmp に切り替え ---
        prevTccl = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {tmp.toUri().toURL()}, prevTccl);
        Thread.currentThread().setContextClassLoader(cl);
        tcclOverridden = true;

        // --- Spring テスト TX に DataSource をバインド（参加させる） ---
        DataSource ds = mock(DataSource.class);
        org.springframework.jdbc.datasource.ConnectionHolder holder =
                new org.springframework.jdbc.datasource.ConnectionHolder(mock(Connection.class));
        org.springframework.transaction.support.TransactionSynchronizationManager.bindResource(ds,
                holder);
        boundDsForCleanup = ds;

        // --- 実行（公開 API 経由） ---
        Method m = testClass.getDeclaredMethod("dummy");
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(testClass).when(ctx).getRequiredTestClass();
        doReturn(Optional.of(m)).when(ctx).getTestMethod();

        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        ext.beforeAll(ctx);

        // ロード途中での例外発生を許容（異常系）。getLoad/getDataPath の return ラインは到達する。
        assertThrows(RuntimeException.class, () -> ext.beforeTestExecution(ctx));

        cl.close();
    }

    @Test
    public void beforeTestExecution_異常ケース_メソッドDB指定_pathsConfigのgetDumpが評価される経路_例外がスローされること(
            @TempDir Path tmp) throws Exception {

        // --- application.properties（dbp2） ---
        Files.writeString(tmp.resolve("application.properties"),
                "spring.datasource.dbp2.url=jdbc:stub:dbp2\n"
                        + "spring.datasource.dbp2.username=userp2\n");

        // --- テスト用のクラス配下にシナリオ/DB ディレクトリを用意 ---
        Class<?> testClass = DummyClass_MethodAnn_SpringBranch_PathGetters.class;
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        Path classRoot = tmp.resolve(pkgPath).resolve(testClass.getSimpleName());
        Files.createDirectories(classRoot.resolve("s_path").resolve("dbp2"));

        // --- TCCL 切り替え ---
        prevTccl = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {tmp.toUri().toURL()}, prevTccl);
        Thread.currentThread().setContextClassLoader(cl);
        tcclOverridden = true;

        // --- Spring テスト TX 参加 ---
        DataSource ds = mock(DataSource.class);
        org.springframework.jdbc.datasource.ConnectionHolder holder =
                new org.springframework.jdbc.datasource.ConnectionHolder(mock(Connection.class));
        org.springframework.transaction.support.TransactionSynchronizationManager.bindResource(ds,
                holder);
        boundDsForCleanup = ds;

        // --- 実行（公開 API 経由） ---
        Method m = testClass.getDeclaredMethod("dummy");
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(testClass).when(ctx).getRequiredTestClass();
        doReturn(Optional.of(m)).when(ctx).getTestMethod();

        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        ext.beforeAll(ctx);

        // 例外は許容。実行後に dump ディレクトリが作られていることを確認。
        assertThrows(RuntimeException.class, () -> ext.beforeTestExecution(ctx));

        Path dumpDir = Path.of(System.getProperty("user.dir"), "target", "dbunit", "dump");
        assertTrue(Files.exists(dumpDir) && Files.isDirectory(dumpDir));

        cl.close();
    }

    @Test
    public void beforeTestExecution_異常ケース_メソッドDB指定_ユーザー大文字化Resolverが評価される経路_例外がスローされること(
            @TempDir Path tmp) throws Exception {

        // --- application.properties（dbu）: username を小文字で設定 ---
        Files.writeString(tmp.resolve("application.properties"),
                "spring.datasource.dbu.url=jdbc:stub:dbu\n"
                        + "spring.datasource.dbu.username=loweruser\n");

        // --- テスト用クラス配下にシナリオ/DB ディレクトリ ---
        Class<?> testClass = DummyClass_MethodAnn_SpringBranch_UserUpper.class;
        String pkgPath = testClass.getPackage().getName().replace('.', '/');
        Path classRoot = tmp.resolve(pkgPath).resolve(testClass.getSimpleName());
        Files.createDirectories(classRoot.resolve("s_upper").resolve("dbu"));

        // --- TCCL 切替 ---
        prevTccl = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(new URL[] {tmp.toUri().toURL()}, prevTccl);
        Thread.currentThread().setContextClassLoader(cl);
        tcclOverridden = true;

        // --- Spring テスト TX 参加 ---
        DataSource ds = mock(DataSource.class);
        org.springframework.jdbc.datasource.ConnectionHolder holder =
                new org.springframework.jdbc.datasource.ConnectionHolder(mock(Connection.class));
        org.springframework.transaction.support.TransactionSynchronizationManager.bindResource(ds,
                holder);
        boundDsForCleanup = ds;

        // --- 実行（公開 API 経由） ---
        Method m = testClass.getDeclaredMethod("dummy");
        ExtensionContext ctx = mock(ExtensionContext.class);
        doReturn(testClass).when(ctx).getRequiredTestClass();
        doReturn(Optional.of(m)).when(ctx).getTestMethod();

        LoadCsvDataExtension ext = new LoadCsvDataExtension();
        ext.beforeAll(ctx);

        // ロード途中の例外は許容。resolver の行は実行経路上で必ず通過する。
        assertThrows(RuntimeException.class, () -> ext.beforeTestExecution(ctx));

        cl.close();
    }
}
