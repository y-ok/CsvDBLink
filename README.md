# FlexDBLink

**FlexDBLink** は、開発・テスト工程における「DBデータの用意と検証」を圧倒的に効率化するデータ管理ツールです。
CSV/JSON/YAML/XML などのシンプルなテキストファイルを使って、データベースへの **一括ロード／ダンプ** を誰でも簡単に実行できます。
さらに BLOB/CLOB などの大容量 LOB データも「外部ファイル参照」で直感的に扱えるため、アプリケーションの実運用データを忠実に再現できます。

CLI によるバッチ投入・取得はもちろん、JUnit 5 拡張機能を利用すれば **テストケース単位でのデータ準備と自動ロールバック** が可能。
これにより「テスト前後のDB状態管理」や「シナリオごとのデータ切り替え」がスムーズになり、手作業のデータ調整から解放されます。
そして最大の強みは、**SQL スクリプトに依存せず、テキストデータとして一元管理できるため、Git などで容易に構成管理・差分追跡・再現性の確保ができること**です。

---

**FlexDBLink** is a powerful yet lightweight tool designed to **streamline database test data management**.
It enables seamless **round-trips between DB and structured files** (CSV/JSON/YAML/XML), making it effortless to load datasets into your database or dump them back into files for inspection and reuse.
Even large **LOBs (BLOB/CLOB)** are handled intuitively through external file references, so you can reproduce production-like data with ease.

Beyond bulk loading/dumping via the CLI, FlexDBLink provides a **JUnit 5 extension** that automatically loads datasets before test execution and rolls back afterward.
This ensures **reliable, reproducible, and isolated test environments**, eliminating the hassle of manual data setup and cleanup.
Most importantly, its greatest strength lies in the fact that **you no longer need to rely on handwritten SQL scripts — datasets are managed as plain text files, enabling easy version control with Git, clear diff tracking, and reproducibility across environments**.

---

## 特長

* **Load & Dump**: CSV / JSON / YAML / XML ファイルから DB へデータロードします。また、DB から CSV/JSON/YAML/XML ファイルとしてデータ取得できます。LOBデータをファイルとして管理し、データロードおよびデータ取得できます。
* **LOB は外部ファイルで**: データセルに `file:...` と書けば、`files/` ディレクトリの実体とリンクします。
* **2 段階ロード**（初期投入 `pre` とシナリオ追加入力）＋ シナリオ時は **既存重複の削除＋新規 INSERT のみ**を実施します。
* **テーブル順序自動化**: `DBUnit` 用の `table-ordering.txt` が無ければ データセット群から自動生成します。
* **JUnit 5 拡張**: `@LoadData` でテストケースごとに データファイルを投入。SpringTestのトランザクション制御に従います。
* **Oracle 対応**: INTERVAL/TIMESTAMP/TZ の正規化、BLOB/CLOB の JDBC 標準 API での投入などを実装しています。

## Features

* **Load & Dump**: Load data from CSV / JSON / YAML / XML files into the DB, and export data from the DB as CSV/JSON/YAML/XML files. LOB data is managed as external files and can be both loaded and exported.
* **External LOB files**: If a dataset cell contains `file:...`, it links to the actual file under the `files/` directory.
* **Two-phase loading**: Initial `pre` load plus scenario-specific additions; during scenarios, **remove existing duplicates and perform INSERT-only**.
* **Automated table ordering**: If `table-ordering.txt` for DBUnit is missing, it’s auto-generated from the dataset set.
* **JUnit 5 extension**: Use `@LoadData` to load datasets per test case; adheres to Spring Test transaction management.
* **Oracle support**: Normalization for INTERVAL/TIMESTAMP/TZ types and LOB insertion via standard JDBC APIs (BLOB/CLOB).

---

## 前提

* **Java 11+**（`JAVA_HOME` を JDK 11 以上に設定）
* **Apache Maven 3.9+**（CLI ツールのビルドに使用します。例: 3.9.10）
* **Oracle JDBC Driver**（`oracle.jdbc.OracleDriver`）
* **OS**: Windows / macOS / Linux いずれでも可

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

* `flexdblink.jar` — 実行 JAR（配布 ZIP に含まれる）
* `FlexDBLink-sources.jar` — ソース JAR
* `FlexDBLink-distribution.zip` — 配布用 ZIP
  （ZIP 展開後は `FlexDBLink/` 直下に `flexdblink.jar` と `conf/application.yml` が配置されます）

---

## CLI の使い方 / CLI Usage

ビルドで生成される **`flexdblink.jar`** を使います。

### コマンド

```bash
# ロード（シナリオ省略時は application.yml の pre-dir-name を使用、通常 "pre"）
java -jar flexdblink.jar --load [<scenario>] --target DB1,DB2

# ダンプ（シナリオ指定が必須）
java -jar flexdblink.jar --dump <scenario> --target DB1,DB2
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

- **[Oracle 19c（Docker）環境構築と FlexDBLink サンプル実行](script/README_jp.md)**
- **[Set Up Oracle 19c (Docker) and Run the FlexDBLink Sample](script/README_en.md)**

### CLI 実行結果

<details>
<summary><strong>ロード実行例</strong>（<code>--load COMMON</code>）</summary>

```bash
$ java -Dspring.config.additional-location=file:conf/ -jar flexdblink.jar --load COMMON

  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
( ( )\___ | '_ | '_| | '_ \/ _` | \ \ \ \
 \\/  ___)| |_)| | | | | || (_| |  ) ) ) )
  '  |____| .__|_| |_|_| |_\__, | / / / /
 =========|_|==============|___/=/_/_/_/
 :: Spring Boot ::               (v2.7.18)

2025-08-17 23:53:28.743  INFO 24625 --- [           main] io.github.yok.flexdblink.Main            : Starting Main v0.1.0 using Java 11.0.27 on okawauchi with PID 24625
2025-08-17 23:53:28.745  INFO 24625 --- [           main] io.github.yok.flexdblink.Main            : No active profile set, falling back to 1 default profile: "default"
2025-08-17 23:53:29.287  INFO 24625 --- [           main] io.github.yok.flexdblink.Main            : Started Main in 0.938 seconds (JVM running for 1.311)
2025-08-17 23:53:29.289  INFO 24625 --- [           main] io.github.yok.flexdblink.Main            : Application started. Args: [--load, COMMON]
2025-08-17 23:53:29.290  INFO 24625 --- [           main] io.github.yok.flexdblink.Main            : Mode: load, Scenario: COMMON, Target DBs: [DB1]
2025-08-17 23:53:29.291  INFO 24625 --- [           main] io.github.yok.flexdblink.Main            : Starting data load. Scenario [COMMON], Target DBs [DB1]
2025-08-17 23:53:29.293  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : === DataLoader started (mode=COMMON, target DBs=[DB1]) ===
2025-08-17 23:53:29.947  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : Generated table-ordering.txt: load/pre/DB1/table-ordering.txt
2025-08-17 23:53:29.953  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Excluded tables: [flyway_schema_history]
2025-08-17 23:53:30.039  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: BINARY_TEST_TABLE.csv
2025-08-17 23:53:30.059  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[BINARY_TEST_TABLE] rows=2
2025-08-17 23:53:30.060  INFO 24625 --- [           main] i.g.y.f.db.OracleDialectHandler          :   Extracting LOB columns from: load/pre/DB1/BINARY_TEST_TABLE.csv
2025-08-17 23:53:30.193  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[BINARY_TEST_TABLE] Initial | inserted=2
2025-08-17 23:53:30.196  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: CHAR_CLOB_TEST_TABLE.csv
2025-08-17 23:53:30.198  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[CHAR_CLOB_TEST_TABLE] rows=2
2025-08-17 23:53:30.198  INFO 24625 --- [           main] i.g.y.f.db.OracleDialectHandler          :   Extracting LOB columns from: load/pre/DB1/CHAR_CLOB_TEST_TABLE.csv
2025-08-17 23:53:30.245  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[CHAR_CLOB_TEST_TABLE] Initial | inserted=2
2025-08-17 23:53:30.247  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: DATE_TIME_TEST_TABLE.csv
2025-08-17 23:53:30.249  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[DATE_TIME_TEST_TABLE] rows=4
2025-08-17 23:53:30.249  INFO 24625 --- [           main] i.g.y.f.db.OracleDialectHandler          :   Extracting LOB columns from: load/pre/DB1/DATE_TIME_TEST_TABLE.csv
2025-08-17 23:53:30.281  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[DATE_TIME_TEST_TABLE] Initial | inserted=4
2025-08-17 23:53:30.283  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: NO_PK_TABLE.csv
2025-08-17 23:53:30.285  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[NO_PK_TABLE] rows=2
2025-08-17 23:53:30.285  INFO 24625 --- [           main] i.g.y.f.db.OracleDialectHandler          :   Extracting LOB columns from: load/pre/DB1/NO_PK_TABLE.csv
2025-08-17 23:53:30.306  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[NO_PK_TABLE] Initial | inserted=2
2025-08-17 23:53:30.308  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: NUMERIC_TEST_TABLE.csv
2025-08-17 23:53:30.310  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[NUMERIC_TEST_TABLE] rows=3
2025-08-17 23:53:30.310  INFO 24625 --- [           main] i.g.y.f.db.OracleDialectHandler          :   Extracting LOB columns from: load/pre/DB1/NUMERIC_TEST_TABLE.csv
2025-08-17 23:53:30.332  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[NUMERIC_TEST_TABLE] Initial | inserted=3
2025-08-17 23:53:30.334  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: SAMPLE_BLOB_TABLE.csv
2025-08-17 23:53:30.336  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[SAMPLE_BLOB_TABLE] rows=2
2025-08-17 23:53:30.336  INFO 24625 --- [           main] i.g.y.f.db.OracleDialectHandler          :   Extracting LOB columns from: load/pre/DB1/SAMPLE_BLOB_TABLE.csv
2025-08-17 23:53:30.358  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[SAMPLE_BLOB_TABLE] Initial | inserted=2
2025-08-17 23:53:30.359  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: VARCHAR2_CHAR_TEST_TABLE.csv
2025-08-17 23:53:30.361  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] rows=6
2025-08-17 23:53:30.361  INFO 24625 --- [           main] i.g.y.f.db.OracleDialectHandler          :   Extracting LOB columns from: load/pre/DB1/VARCHAR2_CHAR_TEST_TABLE.csv
2025-08-17 23:53:30.391  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] Initial | inserted=6
2025-08-17 23:53:30.393  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: XML_JSON_TEST_TABLE.csv
2025-08-17 23:53:30.394  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[XML_JSON_TEST_TABLE] rows=2
2025-08-17 23:53:30.394  INFO 24625 --- [           main] i.g.y.f.db.OracleDialectHandler          :   Extracting LOB columns from: load/pre/DB1/XML_JSON_TEST_TABLE.csv
2025-08-17 23:53:30.423  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[XML_JSON_TEST_TABLE] Initial | inserted=2
2025-08-17 23:53:30.427  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : Generated table-ordering.txt: load/COMMON/DB1/table-ordering.txt
2025-08-17 23:53:30.428  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Excluded tables: [flyway_schema_history]
2025-08-17 23:53:30.502  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: BINARY_TEST_TABLE.csv
2025-08-17 23:53:30.504  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[BINARY_TEST_TABLE] rows=4
2025-08-17 23:53:30.554  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[BINARY_TEST_TABLE] Deleted duplicates by primary key → 2
2025-08-17 23:53:30.558  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[BINARY_TEST_TABLE] Scenario (INSERT only) | inserted=2
2025-08-17 23:53:30.560  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: CHAR_CLOB_TEST_TABLE.csv
2025-08-17 23:53:30.561  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[CHAR_CLOB_TEST_TABLE] rows=2
2025-08-17 23:53:30.580  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[CHAR_CLOB_TEST_TABLE] Deleted duplicates by primary key → 1
2025-08-17 23:53:30.588  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[CHAR_CLOB_TEST_TABLE] Scenario (INSERT only) | inserted=1
2025-08-17 23:53:30.590  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: DATE_TIME_TEST_TABLE.csv
2025-08-17 23:53:30.592  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[DATE_TIME_TEST_TABLE] rows=4
2025-08-17 23:53:30.622  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[DATE_TIME_TEST_TABLE] Deleted duplicates by primary key → 3
2025-08-17 23:53:30.625  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[DATE_TIME_TEST_TABLE] Scenario (INSERT only) | inserted=1
2025-08-17 23:53:30.626  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: NO_PK_TABLE.csv
2025-08-17 23:53:30.628  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[NO_PK_TABLE] rows=4
2025-08-17 23:53:30.736  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[NO_PK_TABLE] Deleted duplicates by all columns → 2
2025-08-17 23:53:30.739  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[NO_PK_TABLE] Scenario (INSERT only) | inserted=2
2025-08-17 23:53:30.741  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: NUMERIC_TEST_TABLE.csv
2025-08-17 23:53:30.742  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[NUMERIC_TEST_TABLE] rows=5
2025-08-17 23:53:30.761  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[NUMERIC_TEST_TABLE] Deleted duplicates by primary key → 3
2025-08-17 23:53:30.765  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[NUMERIC_TEST_TABLE] Scenario (INSERT only) | inserted=2
2025-08-17 23:53:30.766  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: SAMPLE_BLOB_TABLE.csv
2025-08-17 23:53:30.768  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[SAMPLE_BLOB_TABLE] rows=2
2025-08-17 23:53:30.786  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[SAMPLE_BLOB_TABLE] Deleted duplicates by primary key → 1
2025-08-17 23:53:30.790  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[SAMPLE_BLOB_TABLE] Scenario (INSERT only) | inserted=1
2025-08-17 23:53:30.792  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: VARCHAR2_CHAR_TEST_TABLE.csv
2025-08-17 23:53:30.794  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] rows=4
2025-08-17 23:53:30.813  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] Deleted duplicates by primary key → 4
2025-08-17 23:53:30.813  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] Scenario (INSERT only) | inserted=0
2025-08-17 23:53:30.815  INFO 24625 --- [           main] i.g.y.f.parser.DataLoaderFactory         : Resolved dataset file: XML_JSON_TEST_TABLE.csv
2025-08-17 23:53:30.817  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[XML_JSON_TEST_TABLE] rows=1
2025-08-17 23:53:30.833  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[XML_JSON_TEST_TABLE] Deleted duplicates by primary key → 1
2025-08-17 23:53:30.833  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : [DB1] Table[XML_JSON_TEST_TABLE] Scenario (INSERT only) | inserted=0
2025-08-17 23:53:30.836  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : === DataLoader finished ===
2025-08-17 23:53:30.837  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : ===== Summary =====
2025-08-17 23:53:30.837  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : DB[DB1]:
2025-08-17 23:53:30.841  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  :   Table[BINARY_TEST_TABLE       ] Total=2
2025-08-17 23:53:30.841  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  :   Table[CHAR_CLOB_TEST_TABLE    ] Total=2
2025-08-17 23:53:30.841  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  :   Table[DATE_TIME_TEST_TABLE    ] Total=2
2025-08-17 23:53:30.841  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  :   Table[NO_PK_TABLE             ] Total=2
2025-08-17 23:53:30.841  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  :   Table[NUMERIC_TEST_TABLE      ] Total=2
2025-08-17 23:53:30.841  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  :   Table[SAMPLE_BLOB_TABLE       ] Total=2
2025-08-17 23:53:30.841  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  :   Table[VARCHAR2_CHAR_TEST_TABLE] Total=2
2025-08-17 23:53:30.841  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  :   Table[XML_JSON_TEST_TABLE     ] Total=1
2025-08-17 23:53:30.841  INFO 24625 --- [           main] i.github.yok.flexdblink.core.DataLoader  : == Data loading to all DBs has completed ==
2025-08-17 23:53:30.841  INFO 24625 --- [           main] io.github.yok.flexdblink.Main            : Data load completed. Scenario [COMMON]
```

</details>

<details>
<summary><strong>ダンプ例</strong>（<code>--dump COMMON</code>）</summary>

```bash
$ java -Dspring.config.additional-location=file:conf/ -jar flexdblink.jar --dump COMMON

  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
( ( )\___ | '_ | '_| | '_ \/ _` | \ \ \ \
 \\/  ___)| |_)| | | | | || (_| |  ) ) ) )
  '  |____| .__|_| |_|_| |_\__, | / / / /
 =========|_|==============|___/=/_/_/_/
 :: Spring Boot ::               (v2.7.18)

2025-08-17 23:47:06.192  INFO 19573 --- [           main] io.github.yok.flexdblink.Main            : Starting Main v0.1.0 using Java 11.0.27 on okawauchi with PID 19573
2025-08-17 23:47:06.195  INFO 19573 --- [           main] io.github.yok.flexdblink.Main            : No active profile set, falling back to 1 default profile: "default"
2025-08-17 23:47:06.752  INFO 19573 --- [           main] io.github.yok.flexdblink.Main            : Started Main in 0.974 seconds (JVM running for 1.361)
2025-08-17 23:47:06.754  INFO 19573 --- [           main] io.github.yok.flexdblink.Main            : Application started. Args: [--dump, COMMON]
2025-08-17 23:47:06.755  INFO 19573 --- [           main] io.github.yok.flexdblink.Main            : Mode: dump, Scenario: COMMON, Target DBs: [DB1]
2025-08-17 23:47:06.756  INFO 19573 --- [           main] io.github.yok.flexdblink.Main            : Starting data dump. Scenario [COMMON], Target DBs [[DB1]]
2025-08-17 23:47:06.758  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] === DB dump started ===
2025-08-17 23:47:07.559  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[BINARY_TEST_TABLE] CSV dump completed (UTF-8)
2025-08-17 23:47:07.591  INFO 19573 --- [           main] i.g.y.f.db.OracleDialectHandler          :   LOB file written: dump/COMMON/DB1/files/sample3.bin
2025-08-17 23:47:07.593  INFO 19573 --- [           main] i.g.y.f.db.OracleDialectHandler          :   LOB file written: dump/COMMON/DB1/files/sample4.bin
2025-08-17 23:47:07.599  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[BINARY_TEST_TABLE] dumped-records=2, BLOB/CLOB file-outputs=2
2025-08-17 23:47:07.610  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[CHAR_CLOB_TEST_TABLE] CSV dump completed (UTF-8)
2025-08-17 23:47:07.616  INFO 19573 --- [           main] i.g.y.f.db.OracleDialectHandler          :   LOB file written: dump/COMMON/DB1/files/char_clob_2.clob
2025-08-17 23:47:07.617  INFO 19573 --- [           main] i.g.y.f.db.OracleDialectHandler          :   LOB file written: dump/COMMON/DB1/files/char_clob_2.nclob
2025-08-17 23:47:07.618  INFO 19573 --- [           main] i.g.y.f.db.OracleDialectHandler          :   LOB file written: dump/COMMON/DB1/files/char_clob_3.clob
2025-08-17 23:47:07.618  INFO 19573 --- [           main] i.g.y.f.db.OracleDialectHandler          :   LOB file written: dump/COMMON/DB1/files/char_clob_3.nclob
2025-08-17 23:47:07.623  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[CHAR_CLOB_TEST_TABLE] dumped-records=2, BLOB/CLOB file-outputs=4
2025-08-17 23:47:07.662  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[DATE_TIME_TEST_TABLE] CSV dump completed (UTF-8)
2025-08-17 23:47:07.687  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[DATE_TIME_TEST_TABLE] dumped-records=2, BLOB/CLOB file-outputs=0
2025-08-17 23:47:07.693  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[NO_PK_TABLE] CSV dump completed (UTF-8)
2025-08-17 23:47:07.699  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[NO_PK_TABLE] dumped-records=2, BLOB/CLOB file-outputs=0
2025-08-17 23:47:07.708  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[NUMERIC_TEST_TABLE] CSV dump completed (UTF-8)
2025-08-17 23:47:07.713  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[NUMERIC_TEST_TABLE] dumped-records=2, BLOB/CLOB file-outputs=0
2025-08-17 23:47:07.723  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[SAMPLE_BLOB_TABLE] CSV dump completed (UTF-8)
2025-08-17 23:47:07.726  INFO 19573 --- [           main] i.g.y.f.db.OracleDialectHandler          :   LOB file written: dump/COMMON/DB1/files/LeapSecond_3.dat
2025-08-17 23:47:07.727  INFO 19573 --- [           main] i.g.y.f.db.OracleDialectHandler          :   LOB file written: dump/COMMON/DB1/files/LeapSecond_2.dat
2025-08-17 23:47:07.730  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[SAMPLE_BLOB_TABLE] dumped-records=2, BLOB/CLOB file-outputs=2
2025-08-17 23:47:07.739  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] CSV dump completed (UTF-8)
2025-08-17 23:47:07.747  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[VARCHAR2_CHAR_TEST_TABLE] dumped-records=2, BLOB/CLOB file-outputs=0
2025-08-17 23:47:07.756  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[XML_JSON_TEST_TABLE] CSV dump completed (UTF-8)
2025-08-17 23:47:07.760  INFO 19573 --- [           main] i.g.y.f.db.OracleDialectHandler          :   LOB file written: dump/COMMON/DB1/files/json_data_1.json
2025-08-17 23:47:07.760  INFO 19573 --- [           main] i.g.y.f.db.OracleDialectHandler          :   LOB file written: dump/COMMON/DB1/files/xml_data_1.xml
2025-08-17 23:47:07.763  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] Table[XML_JSON_TEST_TABLE] dumped-records=1, BLOB/CLOB file-outputs=2
2025-08-17 23:47:07.764  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : ===== Summary =====
2025-08-17 23:47:07.764  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : DB[DB1]:
2025-08-17 23:47:07.769  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  :   Table[BINARY_TEST_TABLE       ] Total=2
2025-08-17 23:47:07.769  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  :   Table[CHAR_CLOB_TEST_TABLE    ] Total=2
2025-08-17 23:47:07.769  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  :   Table[DATE_TIME_TEST_TABLE    ] Total=2
2025-08-17 23:47:07.769  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  :   Table[NO_PK_TABLE             ] Total=2
2025-08-17 23:47:07.769  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  :   Table[NUMERIC_TEST_TABLE      ] Total=2
2025-08-17 23:47:07.769  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  :   Table[SAMPLE_BLOB_TABLE       ] Total=2
2025-08-17 23:47:07.770  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  :   Table[VARCHAR2_CHAR_TEST_TABLE] Total=2
2025-08-17 23:47:07.770  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  :   Table[XML_JSON_TEST_TABLE     ] Total=1
2025-08-17 23:47:07.770  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : [DB1] === DB dump completed ===
2025-08-17 23:47:07.772  INFO 19573 --- [           main] i.github.yok.flexdblink.core.DataDumper  : === All DB dumps completed: Output [dump/COMMON] ===
2025-08-17 23:47:07.772  INFO 19573 --- [           main] io.github.yok.flexdblink.Main            : Data dump completed. Scenario [COMMON]
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

FlexDBLink の CLI は **`application.yml`** を読み込んで動作します（標準の Spring Boot 外部設定解決に準拠）。
主な探索場所: 実行ディレクトリ直下、`./config/`、クラスパス内 など。

> **Note**: `Main` はコマンドライン引数による Spring のプロパティ上書きを無効化（`setAddCommandLineProperties(false)`）。
> 設定は **YAML ファイル側で管理**してください。

--- 

## Configuration file (`application.yml`)

The FlexDBLink CLI operates by loading **`application.yml`**, adhering to Spring Boot’s standard externalized configuration resolution.
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

`@LoadData` アノテーションを付与すると、テスト実行前に **CSV / JSON / YAML / XML** などのデータファイルを自動投入し、テスト終了時に自動的にロールバックされます。
Spring のテストトランザクション（`@Transactional`）に参加するため、各テストメソッドの終了時点で確実にデータベースの状態が元に戻ります。

`@LoadData` は **クラスレベル** と **メソッドレベル** の両方に付与できます。

* クラスに付与した場合、そのテストクラス全体に対してデータ投入が行われます。
* メソッドに付与した場合、そのテストメソッド専用のデータ投入が行われます。

この仕組みにより、テストデータは **SQL スクリプトを記述せずにテキストファイルで管理**でき、Git などによる構成管理・差分追跡・環境再現が容易になります。

---
## JUnit 5 Support

With the `@LoadData` annotation, **datasets (CSV / JSON / YAML / XML)** are automatically loaded before test execution and rolled back afterward.
Because it integrates with Spring’s test transaction (`@Transactional`), the database state is reliably restored at the end of each test method.

The `@LoadData` annotation can be placed on both the **class level** and the **method level**:

* At the class level, data is loaded once for the entire test class.
* At the method level, data is loaded specifically for that test method.

This allows test data to be managed as plain text files — without writing SQL — enabling easy version control with Git, clear diff tracking, and reproducibility across environments.

---

### Sample （MyBatis マッパーの読み取りテスト）

```java
@MybatisTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(BbbDataSourceDevelopmentConfig.class)
// クラス全体に適用
@LoadData(scenario = "NORMAL", dbNames = DbName.Constants.BBB)
class LeapSecondFileMapperTest {

    @Autowired
    private LeapSecondFileMapper mapper;

    @Test
    // メソッド単位に適用
    @LoadData(scenario = "NORMAL", dbNames = DbName.Constants.BBB)
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

* `@LoadData(scenario = "...", dbNames = "...")`

  * `scenario`: シナリオ（ディレクトリ）名
  * `dbNames`: 1 つまたは複数の DB 名（サブディレクトリ）。省略時は直下の全 DB フォルダを自動検出

--- 

**Resource layout conventions**

```
src/test/resources/<package path>/<TestClassName>/<scenario>/<dbName>/*.csv
src/test/resources/<package path>/<TestClassName>/files/*   # LOB payloads
```

* `@LoadData(scenario = "...", dbNames = "...")`

  * `scenario`: Scenario (directory) name.
  * `dbNames`: One or more DB names (subdirectories). If omitted, all DB folders directly under the scenario are auto-detected.

---

## ベストプラクティス

* **`table-ordering.txt`**
  外部キーなど参照制約を考慮した投入順序を明示化できます（1 行 1 テーブル名）。未配置の場合は、利用するデータファイル群（CSV/JSON/YAML/XML）から自動生成されます。

* **除外テーブル**
  マイグレーション管理テーブルなど、投入やダンプの対象外とすべきテーブルは `dump.exclude-tables` に指定してください。

* **時刻フォーマットの統一**
  日付・時刻のフォーマットは `dbunit.csv.format.*` で統一して設定しておくと、チーム全体でのデータ管理が安定します。

* **テキストデータによる構成管理**
  SQL スクリプトに依存せず、CSV/JSON/YAML/XML でデータを表現することで、Git 等による構成管理・差分追跡・環境再現が容易になります。

---
## Best Practices

* **`table-ordering.txt`**
  Explicitly define the load order with referential constraints in mind (one table name per line). If missing, it is auto-generated from the dataset files (CSV/JSON/YAML/XML).

* **Exclude tables**
  Exclude migration/housekeeping tables by listing them in `dump.exclude-tables`.

* **Unified time formats**
  Configure `dbunit.csv.format.*` for consistent date/time formats across the team.

* **Manage data as text files**
  Instead of relying on SQL scripts, represent datasets as CSV/JSON/YAML/XML files. This enables version control with Git, easy diff tracking, and reproducibility across environments.

---

## 既知事項 / Notes

* 内部で **DBUnit** を利用していますが、API は FlexDBLink で抽象化しています。
* 現状は Oracle に最適化された実装です。その他 RDBMS への拡張は順次対応予定です。

---

* Internally uses **DBUnit**, but the API is abstracted by FlexDBLink.
* The current implementation is optimized for Oracle; support for other RDBMS will be added progressively.

---

## ライセンス / License

本リポジトリは **MIT License** で提供されています。詳しくは [LICENSE](LICENSE.txt) を参照してください。  
This repository is distributed under the **MIT License**. See [LICENSE](LICENSE.txt) for details.


---

## 貢献 / Contributing

バグ報告、機能提案、大歓迎です。Issue / PR をお待ちしています。  
Bug reports and feature requests are very welcome. We look forward to your Issues and PRs.
