#!/bin/bash
# --------------------------------------------------
# 起動ごとに OPEDB の ORACLE スキーマ下にテーブルを作成（存在すればスキップ）
# --------------------------------------------------

sqlplus -s / as sysdba <<EOF
-- 1) PDB を OPEDB に切り替え
ALTER SESSION SET CONTAINER=OPEDB;
-- 2) Current Schema を ORACLE に切り替え
ALTER SESSION SET CURRENT_SCHEMA=ORACLE;

BEGIN
  -- ORACLE スキーマ下で CREATE
  EXECUTE IMMEDIATE q'{
    CREATE TABLE SAMPLE_BLOB_TABLE (
      ID         NUMBER(10)         NOT NULL,
      FILE_NAME  VARCHAR2(255 CHAR) NOT NULL,
      FILE_DATA  BLOB NOT NULL,
      CONSTRAINT PK_SAMPLE_BLOB_TABLE PRIMARY KEY (ID)
    )
  }';
EXCEPTION
  WHEN OTHERS THEN
    -- 既にあればスキップ
    IF SQLCODE != -955 THEN
      RAISE;
    END IF;
END;
/
EXIT;
EOF
