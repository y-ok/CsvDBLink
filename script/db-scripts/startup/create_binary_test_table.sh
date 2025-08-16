#!/bin/bash
# --------------------------------------------------
# 起動ごとに ORACLE スキーマ下に BINARY_TEST_TABLE を作成（存在すればスキップ）
#   カラム：ID, RAW_COL, LONG_RAW_COL, BLOB_COL
# --------------------------------------------------

sqlplus -s / as sysdba <<EOF
-- 1) PDB を OPEDB に切り替え
ALTER SESSION SET CONTAINER=OPEDB;
-- 2) Current Schema を ORACLE に切り替え
ALTER SESSION SET CURRENT_SCHEMA=ORACLE;

BEGIN
  EXECUTE IMMEDIATE q'{
    CREATE TABLE BINARY_TEST_TABLE (
      ID             NUMBER(10)       NOT NULL,
      RAW_COL        RAW(2000),
      LONG_RAW_COL   LONG RAW,
      BLOB_COL       BLOB,
      CONSTRAINT PK_BINARY_TEST PRIMARY KEY (ID)
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
