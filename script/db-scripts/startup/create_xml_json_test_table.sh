#!/bin/bash
# --------------------------------------------------
# 起動ごとに OPEDB の ORACLE スキーマ下に XML_JSON_TEST_TABLE を作成（存在すればスキップ）
#   カラム：ID, JSON_DATA, XML_DATA
# --------------------------------------------------

sqlplus -s / as sysdba <<EOF
-- 1) PDB を OPEDB に切り替え
ALTER SESSION SET CONTAINER=OPEDB;
-- 2) Current Schema を ORACLE に切り替え
ALTER SESSION SET CURRENT_SCHEMA=ORACLE;

BEGIN
  EXECUTE IMMEDIATE q'{
    CREATE TABLE XML_JSON_TEST_TABLE (
      ID        NUMBER          PRIMARY KEY,
      JSON_DATA CLOB            CHECK ( JSON_DATA IS JSON ),
      XML_DATA  CLOB            /* 必要なら XMLTYPE 制約を追加可 */
    )
  }';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -955 THEN
      RAISE;
    END IF;
END;
/
EXIT;
EOF
