# Oracle 19c（Docker）環境構築と FlexDBLink サンプル実行

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

## FlexDBLink を script フォルダに配備

```bash
cd <repo-root>

# ソースからビルドして配置（テスト完全スキップ）
mvn clean package -Dmaven.test.skip=true
cd target
unzip FlexDBLink-distribution.zip

cp -pr FlexDBLink <repo-root>/script/.

# 配置確認
ls -1 <repo-root>/script/FlexDBLink
# conf/
# flexdblink.jar
```

## data-path を修正する手順（application.yml）

**場所**: `script/FlexDBLink/conf/application.yml`

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
└─ FlexDBLink/
   ├─ conf/application.yml
   └─ flexdblink.jar
```

## サンプルデータで実行

```bash
cd script/FlexDBLink

# 初期投入（pre を使用）
java -Dspring.config.additional-location=file:conf/ -jar flexdblink.jar --load

# 追加シナリオ投入（COMMON：重複削除＋INSERT のみ）
java -Dspring.config.additional-location=file:conf/ -jar flexdblink.jar --load COMMON

# ダンプ（COMMON：CSV と LOB ファイルを出力）
java -Dspring.config.additional-location=file:conf/ -jar flexdblink.jar --dump COMMON
```