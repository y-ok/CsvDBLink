# CsvDBLink

CsvDBLink は、CSV とデータベースの往復（ロード／ダンプ）をシンプルにし、BLOB/CLOB 等の LOB も “外部ファイル参照” で扱えるツールです。CLI での一括投入／取得に加えて、JUnit 5 のテスト実行時に CSV を自動投入し、テスト後にロールバックする拡張も提供します。

CsvDBLink simplifies CSV ↔ database round-trips (load/dump) and supports LOBs (BLOB/CLOB) via external file references. In addition to a CLI for bulk loading and dumping, it includes a JUnit 5 extension that auto-loads CSVs during tests and rolls back changes afterward.

---

## 特長

* **Load & Dump**: CSVファイル から DB へデータロードします。また、DB から CSVファイルとしてデータ取得できます。LOBデータをファイルとして管理し、データロードおよびデータ取得できます。
* **LOB は外部ファイルで**: CSV セルに `file:...` と書けば、`files/` ディレクトリの実体とリンクします。
* **2 段階ロード**（初期投入 `pre` とシナリオ追加入力）＋ シナリオ時は **既存重複の削除＋新規 INSERT のみ**を実施します。
* **テーブル順序自動化**: `DBUnit`用の`table-ordering.txt` が無ければ CSV 群から自動生成します。
* **JUnit 5 拡張**: `@LoadCsvData` でテストケースごとに CSV を投入。SpringTestのトランザクション制御に従います。
* **Oracle 対応**: INTERVAL/TIMESTAMP/TZ の正規化、BLOB/CLOB の JDBC 標準 API での投入などを実装しています。

## Features

* **Load & Dump**: Load data from CSV files into the DB, and export data from the DB as CSV files. LOB data is managed as external files and can be both loaded and exported.
* **External LOB files**: If a CSV cell contains `file:...`, it links to the actual file under the `files/` directory.
* **Two-phase loading**: Initial `pre` load plus scenario-specific additions; during scenarios, **remove existing duplicates and perform INSERT-only**.
* **Automated table ordering**: If `table-ordering.txt` for DBUnit is missing, it’s auto-generated from the CSV set.
* **JUnit 5 extension**: Use `@LoadCsvData` to load CSVs per test case; adheres to Spring Test transaction management.
* **Oracle support**: Normalization for INTERVAL/TIMESTAMP/TZ types and LOB insertion via standard JDBC APIs (BLOB/CLOB).

---

## 前提

- **Java 11+**（`JAVA_HOME` を JDK 11 以上に設定）
- **Apache Maven 3.9+**（CLI ツールのビルドに使用します。例: 3.9.10）
- **Oracle JDBC Driver**（`oracle.jdbc.OracleDriver`）
- **OS**: Windows / macOS / Linux いずれでも可

> 現状の方言実装は **Oracle を主対象**に最適化しています。その他 RDB は順次対応予定です。

---

### Requirements

* **Java 11+** (set `JAVA_HOME` to JDK 11 or later)
* **Apache Maven 3.9+** (used to build the CLI tool; e.g., 3.9.10)
* **Oracle JDBC Driver** (`oracle.jdbc.OracleDriver`)
* **OS**: Windows / macOS / Linux

> The current dialect implementation is primarily optimized for **Oracle**. Support for other RDBMSs will be added progressively.

---

## CLI ビルド方法 / CLI Build

```bash
mvn clean package -Dmaven.test.skip=true
```

**Artifacts (in `target/`) / 生成物**

* `csvdblink-0.0.1-all.jar` — Shaded JAR（依存込み／推奨の実行ファイル）
* `CsvDBLink-exec.jar` — Spring Boot fat-jar
* `CsvDBLink.jar` — thin-jar（依存を別途クラスパスに用意する場合）
* `CsvDBLink-sources.jar` — ソース JAR
* `CsvDBLink-distribution.zip` — 配布用 ZIP

## CLI の使い方 / CLI Usage

ビルドで生成される **`csvdblink.jar`** を使います。

### コマンド

```bash
# ロード（シナリオ省略時は application.yml の pre-dir-name を使用、通常 "pre"）
# Load (if the scenario is omitted, use pre-dir-name in application.yml, typically pre)
java -jar csvdblink.jar --load [<scenario>] --target DB1,DB2

# ダンプ（シナリオ指定が必須）
# Dump (scenario name is required)
java -jar csvdblink.jar --dump <scenario> --target DB1,DB2
```

* `--load` / `-l`：ロードモード。`<scenario>` を省略すると `pre` を使用。
* `--dump` / `-d`：ダンプモード。**シナリオ名必須**。
* `--target` / `-t`：対象 DB ID のカンマ区切り。**省略時は `connections[].id` の全件**が対象。

> **注意**: コマンドラインからの Spring プロパティ上書きは無効化しています。**設定は `application.yml` に記述**してください。

--- 

* `--load` / `-l`: Load mode. Uses `pre` if `<scenario>` is omitted.
* `--dump` / `-d`: Dump mode. **Scenario name required**.
* `--target` / `-t`: Comma-separated target DB IDs. **If omitted, all entries in `connections[].id`** are targeted.

> **Note**: Overriding Spring properties from the command line is disabled. **Configure everything in `application.yml`.**

## dockerコンテナを用いたCLI利用手順 / CLI Usage Guide with a Docker Container

- **[Oracle 19c（Docker）環境構築と CsvDBLink サンプル実行](script/README_jp.md)**
- **[Set Up Oracle 19c (Docker) and Run the CsvDBLink Sample](script/README_en.md)**

### CLI 実行結果

<details>
<summary><strong>ロード実行例</strong>（<code>--load COMMON</code>）</summary>

```bash
$ java -Dspring.config.additional-location=file:conf/ -jar csvdblink.jar --load COMMON

  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
( ( )\___ | '_ | '_| | '_ \/ _` | \ \ \ \
 \\/  ___)| |_)| | | | | || (_| |  ) ) ) )
  '  |____| .__|_| |_|_| |_\__, | / / / /
 =========|_|==============|___/=/_/_/_/
 :: Spring Boot ::               (v2.7.18)

2025-08-16 19:44:04.983  INFO 93069 --- [           main] io.github.yok.csvdblink.Main             : Starting Main v0.0.1 using Java 11.0.27 on okawauchi with PID 93069 (CsvDBLink/script/CsvDBLink/csvdblink.jar started by okawauchi in CsvDBLink/script/CsvDBLink)
2025-08-16 19:44:04.987  INFO 93069 --- [           main] io.github.yok.csvdblink.Main             : No active profile set, falling back to 1 default profile: "default"
2025-08-16 19:44:05.540  INFO 93069 --- [           main] io.github.yok.csvdblink.Main             : Started Main in 0.959 seconds (JVM running for 1.308)
2025-08-16 19:44:05.542  INFO 93069 --- [           main] io.github.yok.csvdblink.Main             : Application started. Args: [--load, COMMON]
2025-08-16 19:44:05.543  INFO 93069 --- [           main] io.github.yok.csvdblink.Main             : Mode: load, Scenario: COMMON, Target DBs: [DB1]
2025-08-16 19:44:05.543  INFO 93069 --- [           main] io.github.yok.csvdblink.Main             : Starting data load. Scenario [COMMON], Target DBs [DB1]
2025-08-16 19:44:05.545  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : === DataLoader started (mode=COMMON, target DBs=[DB1]) ===
2025-08-16 19:44:06.193  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : table-ordering.txt already exists (CSV count matches): load/pre/DB1/table-ordering.txt
2025-08-16 19:44:06.214  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Excluded tables: [flyway_schema_history]
2025-08-16 19:44:06.654  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[BINARY_TEST_TABLE] CSV rows=2
2025-08-16 19:44:06.655  INFO 93069 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   Extracting LOB columns from: load/pre/DB1/BINARY_TEST_TABLE.csv
2025-08-16 19:44:08.209  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[BINARY_TEST_TABLE] Initial | inserted=2
2025-08-16 19:44:08.218  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[CHAR_CLOB_TEST_TABLE] CSV rows=2
2025-08-16 19:44:08.218  INFO 93069 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   Extracting LOB columns from: load/pre/DB1/CHAR_CLOB_TEST_TABLE.csv
2025-08-16 19:44:08.276  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[CHAR_CLOB_TEST_TABLE] Initial | inserted=2
2025-08-16 19:44:08.285  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[DATE_TIME_TEST_TABLE] CSV rows=4
2025-08-16 19:44:08.285  INFO 93069 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   Extracting LOB columns from: load/pre/DB1/DATE_TIME_TEST_TABLE.csv
2025-08-16 19:44:08.381  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[DATE_TIME_TEST_TABLE] Initial | inserted=4
2025-08-16 19:44:08.388  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[NO_PK_TABLE] CSV rows=2
2025-08-16 19:44:08.389  INFO 93069 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   Extracting LOB columns from: load/pre/DB1/NO_PK_TABLE.csv
2025-08-16 19:44:08.423  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[NO_PK_TABLE] Initial | inserted=2
2025-08-16 19:44:08.430  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[NUMERIC_TEST_TABLE] CSV rows=3
2025-08-16 19:44:08.430  INFO 93069 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   Extracting LOB columns from: load/pre/DB1/NUMERIC_TEST_TABLE.csv
2025-08-16 19:44:08.455  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[NUMERIC_TEST_TABLE] Initial | inserted=3
2025-08-16 19:44:08.461  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[SAMPLE_BLOB_TABLE] CSV rows=2
2025-08-16 19:44:08.461  INFO 93069 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   Extracting LOB columns from: load/pre/DB1/SAMPLE_BLOB_TABLE.csv
2025-08-16 19:44:08.485  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[SAMPLE_BLOB_TABLE] Initial | inserted=2
2025-08-16 19:44:08.491  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] CSV rows=6
2025-08-16 19:44:08.491  INFO 93069 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   Extracting LOB columns from: load/pre/DB1/VARCHAR2_CHAR_TEST_TABLE.csv
2025-08-16 19:44:08.523  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] Initial | inserted=6
2025-08-16 19:44:08.529  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[XML_JSON_TEST_TABLE] CSV rows=2
2025-08-16 19:44:08.530  INFO 93069 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   Extracting LOB columns from: load/pre/DB1/XML_JSON_TEST_TABLE.csv
2025-08-16 19:44:08.561  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[XML_JSON_TEST_TABLE] Initial | inserted=2
2025-08-16 19:44:08.566  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : table-ordering.txt already exists (CSV count matches): load/COMMON/DB1/table-ordering.txt
2025-08-16 19:44:08.568  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Excluded tables: [flyway_schema_history]
2025-08-16 19:44:08.645  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[BINARY_TEST_TABLE] CSV rows=4
2025-08-16 19:44:08.695  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[BINARY_TEST_TABLE] Deleted duplicates by primary key → 2
2025-08-16 19:44:08.698  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[BINARY_TEST_TABLE] Scenario (INSERT only) | inserted=2
2025-08-16 19:44:08.703  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[CHAR_CLOB_TEST_TABLE] CSV rows=2
2025-08-16 19:44:08.723  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[CHAR_CLOB_TEST_TABLE] Deleted duplicates by primary key → 1
2025-08-16 19:44:08.732  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[CHAR_CLOB_TEST_TABLE] Scenario (INSERT only) | inserted=1
2025-08-16 19:44:08.736  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[DATE_TIME_TEST_TABLE] CSV rows=4
2025-08-16 19:44:08.770  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[DATE_TIME_TEST_TABLE] Deleted duplicates by primary key → 3
2025-08-16 19:44:08.775  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[DATE_TIME_TEST_TABLE] Scenario (INSERT only) | inserted=1
2025-08-16 19:44:08.781  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[NO_PK_TABLE] CSV rows=4
2025-08-16 19:44:08.889  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[NO_PK_TABLE] Deleted duplicates by all columns → 2
2025-08-16 19:44:08.892  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[NO_PK_TABLE] Scenario (INSERT only) | inserted=2
2025-08-16 19:44:08.897  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[NUMERIC_TEST_TABLE] CSV rows=5
2025-08-16 19:44:08.916  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[NUMERIC_TEST_TABLE] Deleted duplicates by primary key → 3
2025-08-16 19:44:08.919  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[NUMERIC_TEST_TABLE] Scenario (INSERT only) | inserted=2
2025-08-16 19:44:08.923  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[SAMPLE_BLOB_TABLE] CSV rows=2
2025-08-16 19:44:08.941  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[SAMPLE_BLOB_TABLE] Deleted duplicates by primary key → 1
2025-08-16 19:44:08.943  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[SAMPLE_BLOB_TABLE] Scenario (INSERT only) | inserted=1
2025-08-16 19:44:08.948  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] CSV rows=4
2025-08-16 19:44:08.967  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] Deleted duplicates by primary key → 4
2025-08-16 19:44:08.967  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] Scenario (INSERT only) | inserted=0
2025-08-16 19:44:08.972  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[XML_JSON_TEST_TABLE] CSV rows=1
2025-08-16 19:44:08.990  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[XML_JSON_TEST_TABLE] Deleted duplicates by primary key → 1
2025-08-16 19:44:08.991  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : [DB1] Table[XML_JSON_TEST_TABLE] Scenario (INSERT only) | inserted=0
2025-08-16 19:44:08.994  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : === DataLoader finished ===
2025-08-16 19:44:08.994  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : ===== Summary =====
2025-08-16 19:44:08.994  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : DB[DB1]:
2025-08-16 19:44:08.998  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  :   Table[BINARY_TEST_TABLE       ] Total=2
2025-08-16 19:44:08.998  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  :   Table[CHAR_CLOB_TEST_TABLE    ] Total=2
2025-08-16 19:44:08.998  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  :   Table[DATE_TIME_TEST_TABLE    ] Total=2
2025-08-16 19:44:08.998  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  :   Table[NO_PK_TABLE             ] Total=2
2025-08-16 19:44:08.998  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  :   Table[NUMERIC_TEST_TABLE      ] Total=2
2025-08-16 19:44:08.998  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  :   Table[SAMPLE_BLOB_TABLE       ] Total=2
2025-08-16 19:44:08.998  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  :   Table[VARCHAR2_CHAR_TEST_TABLE] Total=2
2025-08-16 19:44:08.999  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  :   Table[XML_JSON_TEST_TABLE     ] Total=1
2025-08-16 19:44:08.999  INFO 93069 --- [           main] io.github.yok.csvdblink.core.DataLoader  : == Data loading to all DBs has completed ==
2025-08-16 19:44:08.999  INFO 93069 --- [           main] io.github.yok.csvdblink.Main             : Data load completed. Scenario [COMMON]
```

</details>

<details>
<summary><strong>ダンプ例</strong>（<code>--dump COMMON</code>）</summary>

```bash
$ java -Dspring.config.additional-location=file:conf/ -jar csvdblink.jar --dump COMMON

  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
( ( )\___ | '_ | '_| | '_ \/ _` | \ \ \ \
 \\/  ___)| |_)| | | | | || (_| |  ) ) ) )
  '  |____| .__|_| |_|_| |_\__, | / / / /
 =========|_|==============|___/=/_/_/_/
 :: Spring Boot ::               (v2.7.18)

2025-08-16 19:44:41.924  INFO 93620 --- [           main] io.github.yok.csvdblink.Main             : Starting Main v0.0.1 using Java 11.0.27 on okawauchi with PID 93620 (CsvDBLink/script/CsvDBLink/csvdblink.jar started by okawauchi in CsvDBLink/script/CsvDBLink)
2025-08-16 19:44:41.926  INFO 93620 --- [           main] io.github.yok.csvdblink.Main             : No active profile set, falling back to 1 default profile: "default"
2025-08-16 19:44:42.485  INFO 93620 --- [           main] io.github.yok.csvdblink.Main             : Started Main in 0.953 seconds (JVM running for 1.308)
2025-08-16 19:44:42.487  INFO 93620 --- [           main] io.github.yok.csvdblink.Main             : Application started. Args: [--dump, COMMON]
2025-08-16 19:44:42.488  INFO 93620 --- [           main] io.github.yok.csvdblink.Main             : Mode: dump, Scenario: COMMON, Target DBs: [DB1]
2025-08-16 19:44:42.489  INFO 93620 --- [           main] io.github.yok.csvdblink.Main             : Starting data dump. Scenario [COMMON], Target DBs [[DB1]]
2025-08-16 19:44:42.498  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : Backed up existing dump output directory: dump/COMMON → dump/COMMON_20250816194442490
2025-08-16 19:44:42.498  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] === DB dump started ===
2025-08-16 19:44:44.409  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[BINARY_TEST_TABLE] CSV dump completed (UTF-8)
2025-08-16 19:44:44.445  INFO 93620 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   LOB file written: dump/COMMON/DB1/files/sample3.bin
2025-08-16 19:44:44.447  INFO 93620 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   LOB file written: dump/COMMON/DB1/files/sample4.bin
2025-08-16 19:44:44.452  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[BINARY_TEST_TABLE] dumped-records=2, BLOB/CLOB file-outputs=2
2025-08-16 19:44:44.470  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[CHAR_CLOB_TEST_TABLE] CSV dump completed (UTF-8)
2025-08-16 19:44:44.477  INFO 93620 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   LOB file written: dump/COMMON/DB1/files/char_clob_2.clob
2025-08-16 19:44:44.478  INFO 93620 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   LOB file written: dump/COMMON/DB1/files/char_clob_2.nclob
2025-08-16 19:44:44.480  INFO 93620 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   LOB file written: dump/COMMON/DB1/files/char_clob_3.clob
2025-08-16 19:44:44.481  INFO 93620 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   LOB file written: dump/COMMON/DB1/files/char_clob_3.nclob
2025-08-16 19:44:44.485  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[CHAR_CLOB_TEST_TABLE] dumped-records=2, BLOB/CLOB file-outputs=4
2025-08-16 19:44:44.525  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[DATE_TIME_TEST_TABLE] CSV dump completed (UTF-8)
2025-08-16 19:44:44.565  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[DATE_TIME_TEST_TABLE] dumped-records=2, BLOB/CLOB file-outputs=0
2025-08-16 19:44:44.574  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[NO_PK_TABLE] CSV dump completed (UTF-8)
2025-08-16 19:44:44.582  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[NO_PK_TABLE] dumped-records=2, BLOB/CLOB file-outputs=0
2025-08-16 19:44:44.595  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[NUMERIC_TEST_TABLE] CSV dump completed (UTF-8)
2025-08-16 19:44:44.601  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[NUMERIC_TEST_TABLE] dumped-records=2, BLOB/CLOB file-outputs=0
2025-08-16 19:44:44.614  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[SAMPLE_BLOB_TABLE] CSV dump completed (UTF-8)
2025-08-16 19:44:44.619  INFO 93620 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   LOB file written: dump/COMMON/DB1/files/LeapSecond_3.dat
2025-08-16 19:44:44.620  INFO 93620 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   LOB file written: dump/COMMON/DB1/files/LeapSecond_2.dat
2025-08-16 19:44:44.624  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[SAMPLE_BLOB_TABLE] dumped-records=2, BLOB/CLOB file-outputs=2
2025-08-16 19:44:44.634  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] CSV dump completed (UTF-8)
2025-08-16 19:44:44.645  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] dumped-records=2, BLOB/CLOB file-outputs=0
2025-08-16 19:44:44.657  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[XML_JSON_TEST_TABLE] CSV dump completed (UTF-8)
2025-08-16 19:44:44.662  INFO 93620 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   LOB file written: dump/COMMON/DB1/files/json_data_1.json
2025-08-16 19:44:44.662  INFO 93620 --- [           main] i.g.y.csvdblink.db.OracleDialectHandler  :   LOB file written: dump/COMMON/DB1/files/xml_data_1.xml
2025-08-16 19:44:44.666  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] Table[XML_JSON_TEST_TABLE] dumped-records=1, BLOB/CLOB file-outputs=2
2025-08-16 19:44:44.666  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : ===== Summary =====
2025-08-16 19:44:44.666  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : DB[DB1]:
2025-08-16 19:44:44.671  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  :   Table[BINARY_TEST_TABLE       ] Total=2
2025-08-16 19:44:44.671  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  :   Table[CHAR_CLOB_TEST_TABLE    ] Total=2
2025-08-16 19:44:44.671  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  :   Table[DATE_TIME_TEST_TABLE    ] Total=2
2025-08-16 19:44:44.671  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  :   Table[NO_PK_TABLE             ] Total=2
2025-08-16 19:44:44.671  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  :   Table[NUMERIC_TEST_TABLE      ] Total=2
2025-08-16 19:44:44.671  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  :   Table[SAMPLE_BLOB_TABLE       ] Total=2
2025-08-16 19:44:44.671  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  :   Table[VARCHAR2_CHAR_TEST_TABLE] Total=2
2025-08-16 19:44:44.671  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  :   Table[XML_JSON_TEST_TABLE     ] Total=1
2025-08-16 19:44:44.671  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : [DB1] === DB dump completed ===
2025-08-16 19:44:44.674  INFO 93620 --- [           main] io.github.yok.csvdblink.core.DataDumper  : === All DB dumps completed: Output [dump/COMMON] ===
2025-08-16 19:44:44.674  INFO 93620 --- [           main] io.github.yok.csvdblink.Main             : Data dump completed. Scenario [COMMON]
```

</details>

### ディレクトリ構成（`data-path` 配下）

```text
<data-path>/
  load/
    pre/
      <DB_ID>/
        TABLE_A.csv
        TABLE_B.csv
        table-ordering.txt   # 省略可：無ければ自動生成
                             # Optional: auto-generated if missing
    <scenario-name>/
      <DB_ID>/
        ...
  files/
    ...                      # LOB 実体（CSV からは file:xxx で参照）
                             # LOB payloads (referenced from CSV as file:xxx)
  dump/
    <scenario-name>/
      <DB_ID>/
        *.csv                # ダンプ結果
                             # Dump results
```

### LOB の扱い

* **ロード時**: CSV セルに `file:Foo_001.bin` のように書くと、`<data-path>/files/Foo_001.bin` を読み込んで投入します。
* **ダンプ時**: LOB 列は `<data-path>/files/` に実体を書き出し、CSV 側には `file:...` を出力。ファイル名は `file-patterns` でテーブル／列ごとにテンプレート指定可能（後述）。

---

### Handling LOBs

* **On load**: If a CSV cell contains `file:Foo_001.bin`, the tool reads and inserts `<data-path>/files/Foo_001.bin`.
* **On dump**: LOB columns are written to `<data-path>/files/`, and the CSV contains `file:...` references. Filenames can be templated per table/column via `file-patterns` (see below).

---

## 設定ファイル（`application.yml`）

CsvDBLink の CLI は **`application.yml`** を読み込んで動作します（標準の Spring Boot 外部設定解決に準拠）。
主な探索場所: 実行ディレクトリ直下、`./config/`、クラスパス内 など。

> **Note**: `Main` はコマンドライン引数による Spring のプロパティ上書きを無効化（`setAddCommandLineProperties(false)`）。
> 設定は **YAML ファイル側で管理**してください。

--- 

## Configuration file (`application.yml`)

The CsvDBLink CLI operates by loading **`application.yml`**, adhering to Spring Boot’s standard externalized configuration resolution.
Primary lookup locations include: the working directory, `./config/`, and the classpath.

> **Note**: `Main` disables overriding Spring properties via command-line arguments (`setAddCommandLineProperties(false)`). Please **manage settings in the YAML file**.

### Sample

```yaml
data-path: /absolute/path/to/project-root

dbunit:
  dataTypeFactoryMode: ORACLE
  lob-dir-name: files
  pre-dir-name: pre
  csv:
    format:
      date: "yyyy-MM-dd"
      time: "HH:mm:ss"
      dateTime: "yyyy-MM-dd HH:mm:ss"
      dateTimeWithMillis: "yyyy-MM-dd HH:mm:ss.SSS"
  config:
    allow-empty-fields: true
    batched-statements: true
    batch-size: 100

connections:
  - id: DB1
    url: jdbc:oracle:thin:@localhost:1521/OPEDB
    user: oracle
    password: password
    driver-class: oracle.jdbc.OracleDriver

file-patterns:
  SAMPLE_BLOB_TABLE:
    FILE_DATA: "LeapSecond_{ID}.dat"
  BINARY_TEST_TABLE:
    BLOB_COL: "sample{ID}.bin"
  CHAR_CLOB_TEST_TABLE:
    CLOB_COL: "char_clob_{ID}.clob"
    NCLOB_COL: "char_clob_{ID}.nclob"
  XML_JSON_TEST_TABLE:
    JSON_DATA: "json_data_{ID}.json"
    XML_DATA: "xml_data_{ID}.xml"

dump:
  exclude-tables:
    - flyway_schema_history
```

**主な項目**

* `data-path` (**必須**) — CSV と外部ファイル（LOB）の **ベース絶対パス**。`load/`, `dump/`, `files/` をここから解決。
* `dbunit.*` — 方言・CSV フォーマット・DBUnit 設定。`lob-dir-name` は **`files`** を推奨。
* `connections[]` — CLI が対象とする接続。`id` は `--target`、および `load/<scenario>/<DB_ID>/` と対応。
* `file-patterns` — **ダンプ**時の LOB 出力ファイル名テンプレート（同一行の `{列名}` で置換）。
* `dump.exclude-tables` — ダンプ対象外テーブル（例: `flyway_schema_history`）。

---

**Key items**

* `data-path` (**required**) — **Base absolute path** for CSVs and external (LOB) files. `load/`, `dump/`, and `files/` are resolved from here.
* `dbunit.*` — Dialect, CSV format, and DBUnit settings. It’s recommended to set `lob-dir-name` to **`files`**.
* `connections[]` — Connections targeted by the CLI. Each `id` maps to `--target` and to `load/<scenario>/<DB_ID>/`.
* `file-patterns` — LOB output filename templates used during **dump** (placeholders like `{columnName}` are replaced using values from the same row).
* `dump.exclude-tables` — Tables to exclude from dumping (e.g., `flyway_schema_history`).

---

## CSV 仕様と LOB ファイル

* **ヘッダ付き CSV（UTF-8）**
* 値が `file:xxx` のセルは **`<data-path>/files/xxx` を参照**（ロード時）または ダンプ時同様の形式で出力します。
* データサンプル

  ```csv
  ID,FILE_DATA
  001,file:LeapSecond_001.dat
  002,file:LeapSecond_002.dat
  ```
---

## CSV format and LOB files

* **CSV with header (UTF-8)**
* Cells whose value is `file:xxx` **refer to `<data-path>/files/xxx` when loading**, and the dump emits the same `file:...` form.
* Data sample

  ```csv
  ID,FILE_DATA
  001,file:LeapSecond_001.dat
  002,file:LeapSecond_002.dat
  ```

---

## JUnit 5 サポート

`@LoadCsvData` アノテーションで、テスト実行前に CSV/ファイルを自動投入し、テスト後にロールバックされます。
Spring を利用している場合は、テスト用トランザクション（@Transactional）に参加します。Spring を利用していない場合は、本拡張が独自に接続を管理し、テストクラスの終了時に一括ロールバックします。

---

## JUnit 5 Support

Using the `@LoadCsvData` annotation, CSVs/files are automatically loaded before test execution and rolled back afterward.
When Spring is in use, it participates in the test transaction (`@Transactional`). Without Spring, the extension manages its own connections and performs a bulk rollback at the end of the test class.

### Sample （MyBatis マッパーの読み取りテスト）

```java
@MybatisTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(BbbDataSourceDevelopmentConfig.class)
@LoadCsvData(scenario = "NORMAL", dbNames = DbName.Constants.BBB)
class LeapSecondFileMapperTest {

    @Autowired
    private LeapSecondFileMapper mapper;

    @Test
    void selectLatest_正常ケース_指定fileNameの最新レコードが取得できること() {
        LeapSecondFileRecord record = mapper.selectLatest("test_file.txt");

        assertNotNull(record);
        assertEquals("ID002", record.getIdentifier());
        assertEquals("test_file.txt", record.getFileName());
        assertEquals("2025-08-16T09:00", record.getUpdateTime().toString().substring(0, 16));

        String fileContent = new String(record.getFileData(), StandardCharsets.UTF_8);
        assertEquals("Hello Leap Second", fileContent.trim());
    }
}
```

**リソース配置規約**

```
src/test/resources/<パッケージ階層>/<テストクラス名>/<シナリオ>/<DB名>/*.csv
src/test/resources/<パッケージ階層>/<テストクラス名>/files/*   # LOB 実体
```

* `@LoadCsvData(scenario = "...", dbNames = "...")`

  * `scenario`: シナリオ（ディレクトリ）名
  * `dbNames`: 1 つまたは複数の DB 名（サブディレクトリ）。省略時は直下の全 DB フォルダを自動検出

> Spring の `DataSource` を使わない場合でも、`application.properties` を読み込んで接続を確立します。
> Spring 管理の `DataSource` をツールへ明示提供したい場合は `DataSourceRegistry` も利用できます。

--- 

**Resource layout conventions**

```
src/test/resources/<package path>/<TestClassName>/<scenario>/<dbName>/*.csv
src/test/resources/<package path>/<TestClassName>/files/*   # LOB payloads
```

* `@LoadCsvData(scenario = "...", dbNames = "...")`

  * `scenario`: Scenario (directory) name.
  * `dbNames`: One or more DB names (subdirectories). If omitted, all DB folders directly under the scenario are auto-detected.

> Even when you don’t use Spring’s `DataSource`, the tool reads `application.properties` to establish connections.
> If you want to explicitly provide Spring-managed `DataSource`s to the tool, you can also use `DataSourceRegistry`.

---

## ベストプラクティス

* **`table-ordering.txt`**
  参照制約を考慮した投入順序を明示化できます（1 行 1 テーブル名）。未配置なら CSV から自動生成。
* **除外テーブル**
  `dump.exclude-tables` にマイグレーション管理テーブル等を追加。(CLI用)
* **時刻フォーマットの統一**
  `dbunit.csv.format.*` をチーム標準に合わせて設定。(CLI用)

---

## Best Practices

* **`table-ordering.txt`**
  Explicitly define the load order with referential constraints in mind (one table name per line). If absent, it’s auto-generated from the CSV set.
* **Exclude tables**
  Add migration/housekeeping tables to `dump.exclude-tables` (for CLI).
* **Unified time formats**
  Configure `dbunit.csv.format.*` to match your team’s standard (for CLI).

---

## 既知事項 / Notes

* 内部で **DBUnit** を利用していますが、API は CsvDBLink で抽象化しています。
* 現状は Oracle に最適化された実装です。その他 RDBMS への拡張は順次対応予定です。

---

* Internally uses **DBUnit**, but the API is abstracted by CsvDBLink.
* The current implementation is optimized for Oracle; support for other RDBMS will be added progressively.

---

## ライセンス / License

本リポジトリは **MIT License** で提供されています。詳しくは [LICENSE](LICENSE.txt) を参照してください。  
This repository is distributed under the **MIT License**. See [LICENSE](LICENSE.txt) for details.


---

## 貢献 / Contributing

バグ報告、機能提案、大歓迎です。Issue / PR をお待ちしています。  
Bug reports and feature requests are very welcome. We look forward to your Issues and PRs.
