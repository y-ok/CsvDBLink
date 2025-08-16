# Oracle 19c（Docker）環境構築と CsvDBLink サンプル実行

## Oracle 19c Docker イメージ作成（19.3.0-EE）

```bash
# リポジトリ取得
git clone https://github.com/oracle/docker-images.git
cd docker-images/OracleDatabase/SingleInstance/dockerfiles

# インストーラ ZIP を配置（展開しない）
# Oracle 公式サイトから LINUX.X64_193000_db_home.zip を取得し、19.3.0/ に置く
# 例: cp ~/Downloads/LINUX.X64_193000_db_home.zip 19.3.0/

# イメージをビルド（Enterprise Edition）
chmod +x buildContainerImage.sh
./buildContainerImage.sh -v 19.3.0 -e

# 作成確認（19.3.0-ee が表示されること）
docker images | grep 'oracle/database'
```

## コンテナの起動・停止・ログ

```bash
# 起動
docker compose up -d

# 状態（healthy になるまで待つ）
docker ps

# ログ確認
docker logs -f oracle19c

# 停止
docker compose down

# クリーン削除（ボリュームも削除）
docker compose down -v
```

## データ配置パスを環境変数に設定

```bash
# リポジトリ直下で実行：script/data のフルパスを設定
export CSVDBLINK_DATA_PATH="$(pwd)/script/data"
```

## CsvDBLink を script フォルダに配備

```bash
cd <repo-root>

# ソースからビルドして配置（テスト完全スキップ）
mvn clean package -Dmaven.test.skip=true
cd target
unzip CsvDBLink-distribution.zip

cp -pr CsvDBLink <repo-root>/script/.

# 配置確認
ls -1 <repo-root>/script/CsvDBLink
# conf/
# csvdblink.jar
```

## data-path を修正する手順（application.yml）

**場所**: `script/CsvDBLink/conf/application.yml`

### 変更方法（絶対パスを直書き）

`data-path` に **絶対パス**を直接指定します。

```yaml
data-path: /absolute/path/to/your/<repo-root>/script/data
```

## フォルダ構成

```
script/
├─ data/
│  ├─ load/
│  │  ├─ pre/DB1/{*.csv,files/*}
│  │  └─ COMMON/DB1/{*.csv,files/*}
│  └─ dump/COMMON/DB1/{*.csv,files/*}
└─ CsvDBLink/
   ├─ conf/application.yml
   └─ csvdblink.jar
```

## サンプルデータで実行

```bash
cd script/CsvDBLink

# 初期投入（pre を使用）
java -Dspring.config.additional-location=file:conf/ -jar csvdblink.jar --load

# 追加シナリオ投入（COMMON：重複削除＋INSERT のみ）
java -Dspring.config.additional-location=file:conf/ -jar csvdblink.jar --load COMMON

# ダンプ（COMMON：CSV と LOB ファイルを出力）
java -Dspring.config.additional-location=file:conf/ -jar csvdblink.jar --dump COMMON
```