package io.github.yok.flexdblink.junit;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.util.LogPathUtil;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * JUnit 5 extension that runs {@link ExpectedData} assertions after each test execution.
 *
 * <h2>Expected Directory Layout</h2>
 *
 * <pre>
 * ${pathsConfig.load}/&lt;scenario&gt;/expected/&lt;DB ID&gt;/&lt;table&gt;.(csv|json|yaml|yml|xml)
 * </pre>
 *
 * <p>
 * DB ID is the folder name directly under {@code expected/}. LOB cells in expected files may use
 * {@code file:} and are resolved by basename only:
 * {@code file:x/y/z.ext -> expected/&lt;DB&gt;/files/z.ext}. On LOB mismatches, the actual DB value
 * is dumped into {@code expected/&lt;DB&gt;/actual/z.ext} (UTF-8 for text).
 * </p>
 */
@Slf4j
public class ExpectedDataExtension implements AfterTestExecutionCallback {

    // Spring管理のBean（no-arg拡張のため実行時に解決）
    private PathsConfig pathsConfig;
    private ConnectionConfig connectionConfig;

    // テスト用リソースコンテキスト（classRoot / application*.properties 等）
    private TestResourceContext trc;

    // no-arg（JUnit 5 によるインスタンス化のため必須）
    public ExpectedDataExtension() {
        // no-op
    }

    /**
     * 各テスト実行後に呼ばれ、ExpectedData の突合せを実施します。
     *
     * @param context JUnit 実行コンテキスト
     * @throws Exception 比較失敗または処理失敗時に送出
     */
    @Override
    public void afterTestExecution(ExtensionContext context) throws Exception {

        final String className = context.getRequiredTestClass().getName();
        final String methodName = context.getTestMethod().map(m -> m.getName()).orElse("(unknown)");
        log.info("ExpectedData 拡張を起動しました: class={}, method={}", className, methodName);

        // アノテーション解決（メソッド優先、なければクラス）
        ExpectedData ann = context.getTestMethod().map(m -> m.getAnnotation(ExpectedData.class))
                .orElse(context.getRequiredTestClass().getAnnotation(ExpectedData.class));
        if (ann == null) {
            log.info("@ExpectedData が付与されていないため、突合せをスキップします。");
            return;
        }
        log.info("@ExpectedData を検出しました。excludeColumns={}", String.join(",", ann.excludeColumns()));

        // Spring ApplicationContext の取得
        final ApplicationContext appCtx;
        try {
            appCtx = SpringExtension.getApplicationContext(context);
        } catch (Exception e) {
            log.error("Spring の ApplicationContext を取得できません。比較処理を実施できないためスキップします。", e);
            return;
        }

        // TestResourceContext の初期化
        try {
            if (this.trc == null) {
                this.trc = TestResourceContext.init(context);
            }
        } catch (Exception e) {
            log.error("TestResourceContext の初期化に失敗しました。比較処理を実施できないためスキップします。", e);
            return;
        }

        // PathsConfig の解決（なければフォールバック）
        final AtomicBoolean pathsFromFallback = new AtomicBoolean(false);
        this.pathsConfig = resolvePathsConfigOrFallback(appCtx, pathsFromFallback, context);
        if (pathsFromFallback.get()) {
            log.warn("PathsConfig Bean が見つからないため、フォールバックのパスを使用します。 load={}, dump={}",
                    pathsConfig.getLoad(), pathsConfig.getDump());
        } else {
            log.info("PathsConfig を解決しました。 load={}, dump={}", pathsConfig.getLoad(),
                    pathsConfig.getDump());
        }

        // ConnectionConfig の解決（なければ null のまま。TRC の properties で補完）
        final AtomicBoolean connFromFallback = new AtomicBoolean(false);
        this.connectionConfig = resolveConnectionConfigOrNull(appCtx, connFromFallback);
        if (this.connectionConfig == null) {
            log.warn("ConnectionConfig Bean が見つからないため、properties から URL/USER を解決します。");
        } else {
            log.info("ConnectionConfig を解決しました。接続定義数={}", connectionConfig.getConnections().size());
        }

        // シナリオ名の決定（ブランク時はテストクラス名）
        String scenario = ann.scenario();
        if (StringUtils.isBlank(scenario)) {
            scenario = context.getRequiredTestClass().getSimpleName();
        }
        log.info("シナリオ名を確定: scenario={}", scenario);

        // expected ルートの決定と存在確認
        File expectedRoot = new File(pathsConfig.getLoad(), scenario + File.separator + "expected");
        String expectedRootForLog =
                LogPathUtil.renderDirForLog(new File(expectedRoot.getAbsolutePath()));
        log.info("expected ディレクトリを探索します: {}", expectedRootForLog);
        if (!expectedRoot.exists() || !expectedRoot.isDirectory()) {
            log.warn("expected ルートが存在しないため、突合せをスキップします: {}", expectedRootForLog);
            return;
        }

        // === 対象DB ID の決定 ===
        // @ExpectedData(dbNames=...) が指定されていればそれを使用。
        // なければ ConnectionConfig のエントリ、さらにダメなら expected/ 直下のフォルダ名を走査。
        List<String> targetDbIds = new ArrayList<>();
        if (ann.dbNames().length > 0) {
            targetDbIds.addAll(Arrays.asList(ann.dbNames()));
            log.info("@ExpectedData の dbNames 指定を使用します: {}", targetDbIds);
        } else if (this.connectionConfig != null && this.connectionConfig.getConnections() != null
                && !this.connectionConfig.getConnections().isEmpty()) {
            for (ConnectionConfig.Entry e : this.connectionConfig.getConnections()) {
                targetDbIds.add(e.getId());
            }
            log.info("dbNames 未指定のため ConnectionConfig の定義を使用します: {}", targetDbIds);
        } else {
            targetDbIds.addAll(listDbIdsFromFolder(expectedRoot.toPath()));
            log.info("dbNames 未指定かつ ConnectionConfig 無しのため、expected/ 配下のフォルダから検出しました: {}",
                    targetDbIds);
        }

        if (targetDbIds.isEmpty()) {
            log.warn("検証対象の DB ID が空です。突合せは実行されません。");
            return;
        }

        // 各 DB ごとに比較を実施
        for (String dbId : targetDbIds) {
            File dbExpectedDir = new File(expectedRoot, dbId);
            String dbExpectedForLog =
                    LogPathUtil.renderDirForLog(new File(dbExpectedDir.getAbsolutePath()));
            log.info("DBごとの expected ディレクトリ: {}", dbExpectedForLog);

            if (!dbExpectedDir.exists() || !dbExpectedDir.isDirectory()) {
                log.warn("当該DBの expected データディレクトリが存在しないためスキップします: dbId={}", dbId);
                continue;
            }

            // (1) 当該 dbId の期待URL/USERを解決
            ConnectionConfig.Entry entry = findEntryForDbId(dbId);
            if (entry == null || StringUtils.isBlank(entry.getUser())
                    || StringUtils.isBlank(entry.getUrl())) {
                throw new IllegalStateException("dbId=" + dbId
                        + " の接続情報(URL/USER)を解決できません。properties もしくは ConnectionConfig を確認してください。");
            }

            // (2) 実際の DataSource をメタ情報（URL/USER）から特定（Bean名非依存）
            DataSource ds = resolveDataSourceForDbId(appCtx, dbId, entry.getUrl(), entry.getUser());

            // (3) 比較
            try (Connection jdbc = ds.getConnection()) {
                final String schema =
                        Objects.requireNonNullElse(entry.getUser(), "").toUpperCase(Locale.ROOT);

                log.info("比較を開始します: dbId={}, schema={}, expected={}", dbId, schema,
                        dbExpectedForLog);

                // 差分がある場合、ExpectedDataAssert が詳細ログ出力し AssertionError を送出
                ExpectedDataAssert.assertMatches(jdbc, schema, dbExpectedDir, ann.excludeColumns());

                log.info("比較が正常終了しました（差分なし）: dbId={}", dbId);
            } catch (AssertionError ae) {
                log.error("差分を検出しました（テスト失敗）: dbId={}", dbId);
                throw ae;
            } catch (Exception e) {
                log.error("比較処理中にエラーが発生しました: dbId=" + dbId, e);
                throw e;
            }
        }

        log.info("すべての DB に対する比較が完了しました。");
    }

    /**
     * PathsConfig を Spring から解決。見つからない場合はフォールバックを生成します。
     *
     * フォールバック方針:
     * <ul>
     * <li>load/dataPath: {@code classRoot}</li>
     * <li>dump: {@code target/dbunit/dump}</li>
     * </ul>
     */
    private PathsConfig resolvePathsConfigOrFallback(ApplicationContext appCtx,
            AtomicBoolean usedFallback, ExtensionContext context) {
        try {
            return appCtx.getBean(PathsConfig.class);
        } catch (Exception ex) {
            usedFallback.set(true);
            final Path classRoot = trc.getClassRoot().toAbsolutePath().normalize();
            final String load = classRoot.toString();
            final String dump =
                    new File("target" + File.separator + "dbunit" + File.separator + "dump")
                            .getAbsolutePath();

            // // フォールバックの匿名実装を返す
            return new PathsConfig() {
                @Override
                public String getLoad() {
                    return load;
                }

                @Override
                public String getDataPath() {
                    return load;
                }

                @Override
                public String getDump() {
                    return dump;
                }
            };
        }
    }

    // ConnectionConfig を取得（無ければ null）
    private ConnectionConfig resolveConnectionConfigOrNull(ApplicationContext appCtx,
            AtomicBoolean usedFallback) {
        try {
            return appCtx.getBean(ConnectionConfig.class);
        } catch (Exception ex) {
            usedFallback.set(true);
            return null;
        }
    }

    // 指定 dbId の ConnectionConfig.Entry を選択。なければ TRC properties から構築。
    private ConnectionConfig.Entry findEntryForDbId(String dbId) {
        if (this.connectionConfig != null && this.connectionConfig.getConnections() != null) {
            for (ConnectionConfig.Entry e : this.connectionConfig.getConnections()) {
                if (dbId.equals(e.getId())) {
                    return e;
                }
            }
        }
        return trc.buildEntryFromProps(dbId);
    }

    // --------------------------------------------------------------------------------------
    // DB/DS 解決ヘルパ（メタ情報ベース、Bean名に依存しない）
    // --------------------------------------------------------------------------------------

    /**
     * 指定 dbId 用の DataSource を URL/USER のメタ情報から一意に解決します。 TransactionManager
     * は一意性検証の目的で内部的に探索しますが、保持はしません。
     */
    private DataSource resolveDataSourceForDbId(ApplicationContext ac, String dbId,
            String expectedUrl, String expectedUser) {

        // まず TM 側から解決を試行（一意ならその DS を採用）
        List<TmWithDs> tmMatches = findTmByMetadata(ac, expectedUrl, expectedUser);
        if (tmMatches.size() == 1) {
            TmWithDs m = tmMatches.get(0);
            log.info("[ExpectedData] TM から一意に特定できました: dbId={}, tm={}, ds={}", dbId, m.tmName,
                    m.dsName);
            return m.ds;
        }
        if (tmMatches.size() > 1) {
            List<String> names = new ArrayList<>();
            for (TmWithDs m : tmMatches) {
                names.add(m.tmName);
            }
            throw new IllegalStateException("dbId=" + dbId
                    + " に対して一致する TransactionManager が複数見つかりました。 candidates=" + names);
        }

        // DS 側から解決
        List<NamedDs> dsMatches = findDataSourceByMetadata(ac, expectedUrl, expectedUser);
        if (dsMatches.isEmpty()) {
            String[] all = ac.getBeanNamesForType(DataSource.class);
            throw new IllegalStateException("dbId=" + dbId + " に一致する DataSource が見つかりません。"
                    + " expectedUrl=" + expectedUrl + ", expectedUser=" + expectedUser
                    + " candidates=" + Arrays.toString(all));
        }
        if (dsMatches.size() == 1) {
            NamedDs only = dsMatches.get(0);
            // // 一意性検証：この DS を管理する TM がちょうど1つ存在することを確認（結果は保持しない）
            resolveTxManagerByDataSource(ac, dbId, only.ds);
            return only.ds;
        }

        // 単一の @Primary を優先
        NamedDs primary = pickPrimary(ac, dsMatches);
        if (primary != null) {
            resolveTxManagerByDataSource(ac, dbId, primary.ds);
            log.info("[ExpectedData] @Primary の DataSource を採用しました: dbId={}, ds={}", dbId,
                    primary.name);
            return primary.ds;
        }

        // いずれかの TM から参照されている DS がちょうど1つだけならそれを採用
        NamedDs referencedSingle = pickSingleReferencedByTm(ac, dsMatches);
        if (referencedSingle != null) {
            resolveTxManagerByDataSource(ac, dbId, referencedSingle.ds);
            log.info("[ExpectedData] TM から参照されている唯一の DS を採用しました: dbId={}, ds={}", dbId,
                    referencedSingle.name);
            return referencedSingle.ds;
        }

        // まだ曖昧
        List<String> names = new ArrayList<>();
        for (NamedDs n : dsMatches) {
            names.add(n.name);
        }
        throw new IllegalStateException(
                "dbId=" + dbId + " に一致する DataSource が複数見つかりました。 candidates=" + names);
    }

    // URL/USER メタ一致の TM を探索（DS も併記）
    private List<TmWithDs> findTmByMetadata(ApplicationContext ac, String expectedUrl,
            String expectedUser) {
        List<TmWithDs> result = new ArrayList<>();
        String[] tmNames = ac.getBeanNamesForType(PlatformTransactionManager.class);
        for (String tmName : tmNames) {
            PlatformTransactionManager tm = ac.getBean(tmName, PlatformTransactionManager.class);
            if (tm instanceof DataSourceTransactionManager) {
                DataSource ds = ((DataSourceTransactionManager) tm).getDataSource();
                if (ds == null) {
                    continue;
                }
                ProbeMeta meta = probeDataSourceMeta(ds);
                if (meta == null) {
                    continue;
                }
                if (urlRoughMatch(expectedUrl, meta.url)
                        && equalsIgnoreCaseSafe(expectedUser, meta.user)) {
                    String dsName = findBeanNameByInstance(ac, DataSource.class, ds);
                    result.add(new TmWithDs(tmName, dsName, ds));
                }
            }
        }
        return result;
    }

    // URL/USER メタ一致の DS を探索
    private List<NamedDs> findDataSourceByMetadata(ApplicationContext ac, String expectedUrl,
            String expectedUser) {
        List<NamedDs> result = new ArrayList<>();
        String[] dsNames = ac.getBeanNamesForType(DataSource.class);
        for (String name : dsNames) {
            DataSource ds = ac.getBean(name, DataSource.class);
            ProbeMeta meta = probeDataSourceMeta(ds);
            if (meta == null) {
                continue;
            }
            if (urlRoughMatch(expectedUrl, meta.url)
                    && equalsIgnoreCaseSafe(expectedUser, meta.user)) {
                result.add(new NamedDs(name, ds));
            }
        }
        return result;
    }

    // 候補の中から単一の @Primary DS を返す
    private NamedDs pickPrimary(ApplicationContext ac, List<NamedDs> candidates) {
        if (ac instanceof ConfigurableApplicationContext) {
            ConfigurableListableBeanFactory bf =
                    ((ConfigurableApplicationContext) ac).getBeanFactory();
            NamedDs found = null;
            for (NamedDs n : candidates) {
                if (bf.containsBeanDefinition(n.name)) {
                    BeanDefinition bd = bf.getBeanDefinition(n.name);
                    if (bd.isPrimary()) {
                        if (found != null) {
                            // 複数の @Primary は曖昧扱い
                            return null;
                        }
                        found = n;
                    }
                }
            }
            return found;
        }
        return null;
    }

    // DS 候補のうち、TM から参照されているものがちょうど1つならそれを返す
    private NamedDs pickSingleReferencedByTm(ApplicationContext ac, List<NamedDs> candidates) {
        String[] tmNames = ac.getBeanNamesForType(PlatformTransactionManager.class);
        NamedDs only = null;
        for (NamedDs n : candidates) {
            boolean referenced = false;
            for (String tmName : tmNames) {
                PlatformTransactionManager tm =
                        ac.getBean(tmName, PlatformTransactionManager.class);
                if (tm instanceof DataSourceTransactionManager) {
                    DataSource tmDs = ((DataSourceTransactionManager) tm).getDataSource();
                    if (tmDs == n.ds) {
                        referenced = true;
                        break;
                    }
                }
            }
            if (referenced) {
                if (only != null) {
                    // 2件以上参照 → 曖昧
                    return null;
                }
                only = n;
            }
        }
        return only;
    }

    /**
     * 指定 DS を管理する DataSourceTransactionManager を参照等価で一意に特定します。 一意でない場合は例外。戻り値は検証目的であり、保持しません。
     */
    private PlatformTransactionManager resolveTxManagerByDataSource(ApplicationContext ac,
            String dbId, DataSource ds) {
        String[] tmNames = ac.getBeanNamesForType(PlatformTransactionManager.class);
        List<String> hits = new ArrayList<>();
        PlatformTransactionManager matched = null;

        for (String name : tmNames) {
            PlatformTransactionManager tm = ac.getBean(name, PlatformTransactionManager.class);
            if (tm instanceof DataSourceTransactionManager) {
                DataSource tmDs = ((DataSourceTransactionManager) tm).getDataSource();
                if (tmDs == ds) {
                    hits.add(name);
                    matched = tm;
                }
            }
        }

        if (hits.isEmpty()) {
            throw new IllegalStateException(
                    "対象 DataSource を管理する DataSourceTransactionManager が見つかりませんでした。dbId=" + dbId
                            + " candidates=" + Arrays.toString(tmNames));
        }
        if (hits.size() > 1) {
            throw new IllegalStateException(
                    "対象 DataSource を管理する TransactionManager が複数見つかりました。dbId=" + dbId
                            + " candidates=" + hits);
        }
        return matched;
    }

    // DataSource から接続メタ情報（URL/USER）を取得。失敗時は null を返す。
    private ProbeMeta probeDataSourceMeta(DataSource ds) {
        try (Connection c = ds.getConnection()) {
            DatabaseMetaData md = c.getMetaData();
            String url = (md != null) ? md.getURL() : null;
            String user = (md != null) ? md.getUserName() : null;
            return new ProbeMeta(url, user);
        } catch (Exception e) {
            log.debug("DataSource のメタ情報取得に失敗しました。", e);
            return null;
        }
    }

    // URL を簡略化して包含判定（クエリ/セミコロン除去 + 小文字化）
    private boolean urlRoughMatch(String expected, String actual) {
        String e = simplifyUrl(expected);
        String a = simplifyUrl(actual);
        if (e.isEmpty() || a.isEmpty()) {
            return false;
        }
        return a.contains(e) || e.contains(a);
    }

    // URL の簡略化（クエリ/セミコロン除去 + 小文字化）
    private String simplifyUrl(String url) {
        if (url == null) {
            return "";
        }
        String s = url;
        int q = s.indexOf('?');
        if (q >= 0) {
            s = s.substring(0, q);
        }
        int sc = s.indexOf(';');
        if (sc >= 0) {
            s = s.substring(0, sc);
        }
        return s.toLowerCase(Locale.ROOT).trim();
    }

    // Null 安全な大文字小文字無視比較
    private boolean equalsIgnoreCaseSafe(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        return a.equalsIgnoreCase(b);
    }

    // 参照等価で Bean 名を解決
    private <T> String findBeanNameByInstance(ApplicationContext ac, Class<T> type, T instance) {
        String[] names = ac.getBeanNamesForType(type);
        for (String name : names) {
            T bean = ac.getBean(name, type);
            if (bean == instance) {
                return name;
            }
        }
        return "<unknown>";
    }

    // expected/ 直下のサブディレクトリ名（= DB ID）を列挙
    private Set<String> listDbIdsFromFolder(Path expectedRoot) throws Exception {
        if (!Files.isDirectory(expectedRoot)) {
            return java.util.Collections.emptySet();
        }
        try {
            Set<String> result = new LinkedHashSet<>();
            try (var stream = Files.list(expectedRoot)) {
                stream.filter(Files::isDirectory).map(p -> p.getFileName().toString())
                        .forEach(result::add);
            }
            return result;
        } catch (Exception e) {
            throw new Exception("expected/ 配下の DB ID フォルダ走査に失敗しました: " + expectedRoot, e);
        }
    }

    // --------------------------------------------------------------------------------------
    // 小さなデータホルダ
    // --------------------------------------------------------------------------------------

    // TM と DS の名前/インスタンスのペア
    private static final class TmWithDs {
        final String tmName;
        final String dsName;
        final DataSource ds;

        TmWithDs(String tmName, String dsName, DataSource ds) {
            this.tmName = tmName;
            this.dsName = dsName;
            this.ds = ds;
        }
    }

    // 名前付き DataSource ラッパ
    private static final class NamedDs {
        final String name;
        final DataSource ds;

        NamedDs(String name, DataSource ds) {
            this.name = name;
            this.ds = ds;
        }
    }

    // DS 接続メタ情報コンテナ（URL/USER）
    private static final class ProbeMeta {
        final String url;
        final String user;

        ProbeMeta(String url, String user) {
            this.url = url;
            this.user = user;
        }
    }
}
