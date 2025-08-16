#!/bin/bash
# --------------------------------------------------
# 起動ごとに OPEDB の ORACLE スキーマ下に NUMERIC_TEST_TABLE を作成（存在すればスキップ）
#   カラム：ID, NUM_COL, FLOAT_COL, BIN_FLOAT_COL, BIN_DOUBLE_COL
# --------------------------------------------------

sqlplus -s / as sysdba <<EOF
-- 1) PDB を OPEDB に切り替え
ALTER SESSION SET CONTAINER=OPEDB;
-- 2) Current Schema を ORACLE に切り替え
ALTER SESSION SET CURRENT_SCHEMA=ORACLE;

BEGIN
  EXECUTE IMMEDIATE q'{
    CREATE TABLE NUMERIC_TEST_TABLE (
      ID               NUMBER(10)       NOT NULL,
      NUM_COL          NUMBER(15,2),
      FLOAT_COL        FLOAT(126),
      BIN_FLOAT_COL    BINARY_FLOAT,
      BIN_DOUBLE_COL   BINARY_DOUBLE,
      CONSTRAINT PK_NUMERIC_TEST PRIMARY KEY (ID)
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
