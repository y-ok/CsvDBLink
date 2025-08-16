package io.github.yok.csvdblink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.yok.csvdblink.config.ConnectionConfig;
import io.github.yok.csvdblink.config.DbUnitConfig;
import io.github.yok.csvdblink.config.DumpConfig;
import io.github.yok.csvdblink.config.FilePatternConfig;
import io.github.yok.csvdblink.config.PathsConfig;
import io.github.yok.csvdblink.core.DataDumper;
import io.github.yok.csvdblink.core.DataLoader;
import io.github.yok.csvdblink.db.DbDialectHandler;
import io.github.yok.csvdblink.db.DbDialectHandlerFactory;
import io.github.yok.csvdblink.db.DbUnitConfigFactory;
import io.github.yok.csvdblink.util.ErrorHandler;
import io.github.yok.csvdblink.util.OracleDateTimeFormatUtil;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.springframework.boot.SpringApplication;

/**
 * Unit tests for {@link Main}.
 */
class MainTest {

    private PathsConfig pathsConfig;
    private DbUnitConfig dbUnitConfig;
    private ConnectionConfig connectionConfig;
    private FilePatternConfig filePatternConfig;
    private DumpConfig dumpConfig;
    private DbDialectHandlerFactory dialectFactory;
    private OracleDateTimeFormatUtil dateTimeFormatter;
    private DbUnitConfigFactory configFactory;

    private Main main;

    @BeforeEach
    void setup() {
        pathsConfig = mock(PathsConfig.class);
        dbUnitConfig = mock(DbUnitConfig.class);

        connectionConfig = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setUser("user1");
        connectionConfig.setConnections(Collections.singletonList(entry));

        filePatternConfig = mock(FilePatternConfig.class);
        dumpConfig = mock(DumpConfig.class);
        dialectFactory = mock(DbDialectHandlerFactory.class);
        dateTimeFormatter = mock(OracleDateTimeFormatUtil.class);
        configFactory = mock(DbUnitConfigFactory.class);

        when(dialectFactory.create(any())).thenReturn(mock(DbDialectHandler.class));
        when(dbUnitConfig.getPreDirName()).thenReturn("preScenario");

        main = new Main(pathsConfig, dbUnitConfig, connectionConfig, filePatternConfig, dumpConfig,
                dialectFactory, dateTimeFormatter, configFactory);
    }

    @Test
    void main_正常ケース_SpringApplicationが起動されること() {
        try (MockedConstruction<SpringApplication> mocked =
                mockConstruction(SpringApplication.class, (mock, ctx) -> {
                    // run(String...) をスタブ
                    when(mock.run(any(String[].class))).thenReturn(null);

                    // コンストラクタ引数を検証
                    Object arg0 = ctx.arguments().get(0);
                    assertTrue(arg0 instanceof Class<?>[]);
                    Class<?>[] sources = (Class<?>[]) arg0;
                    assertEquals(1, sources.length);
                    assertEquals(Main.class, sources[0]);
                })) {

            Main.main(new String[] {"--load", "myscenario"});

            SpringApplication app = mocked.constructed().get(0);

            // addCommandLineProperties(false) が呼ばれたこと
            verify(app).setAddCommandLineProperties(false);

            // run(String...) が呼ばれたこと
            verify(app).run(any(String[].class));
        }
    }

    @Test
    void run_異常ケース_シナリオ未指定時にErrorHandlerが呼ばれること() {
        Main sut = new Main(mock(PathsConfig.class), mock(DbUnitConfig.class),
                mock(ConnectionConfig.class), mock(FilePatternConfig.class), mock(DumpConfig.class),
                mock(DbDialectHandlerFactory.class), mock(OracleDateTimeFormatUtil.class),
                mock(DbUnitConfigFactory.class));

        try (MockedStatic<ErrorHandler> mocked = mockStatic(ErrorHandler.class)) {
            mocked.when(() -> ErrorHandler.errorAndExit(anyString())).thenAnswer(inv -> null);

            // dumpモードでシナリオ省略 → 1引数版が呼ばれる
            sut.run("--dump");

            mocked.verify(
                    () -> ErrorHandler.errorAndExit(eq("Scenario name is required in dump mode.")));
        }
    }

    @Test
    void run_正常ケース_引数なしはデフォルトでloadが実行されること() {
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class,
                (mock, ctx) -> doNothing().when(mock).execute(anyString(), anyList()))) {

            main.run();

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("preScenario"), eq(List.of("db1")));
        }
    }

    @Test
    void run_正常ケース_loadシナリオ指定が実行されること() {
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("--load", "myscenario");

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("db1")));
        }
    }

    @Test
    void run_正常ケース_dumpシナリオ指定が実行されること() {
        try (MockedConstruction<DataDumper> mocked = mockConstruction(DataDumper.class)) {
            main.run("--dump", "myscenario");

            DataDumper dumper = mocked.constructed().get(0);
            verify(dumper).execute(eq("myscenario"), eq(List.of("db1")));
        }
    }

    @Test
    void run_正常ケース_target指定が優先されること() {
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("--load", "myscenario", "--target", "dbX,dbY");

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("myscenario"), eq(List.of("dbX", "dbY")));
        }
    }

    @Test
    void run_正常ケース_未知の引数はwarnされても処理継続すること() {
        try (MockedConstruction<DataLoader> mocked = mockConstruction(DataLoader.class)) {
            main.run("--unknown", "xxx");

            DataLoader loader = mocked.constructed().get(0);
            verify(loader).execute(eq("preScenario"), eq(List.of("db1")));
        }
    }
}
