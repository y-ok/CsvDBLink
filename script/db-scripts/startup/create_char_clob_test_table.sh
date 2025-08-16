#!/bin/bash
# --------------------------------------------------
# 起動ごとに ORACLE スキーマ下に CHAR_CLOB_TEST_TABLE を作成（存在すればスキップ）
#   カラム：ID, CHAR_COL, NCHAR_COL, VARCHAR2_COL, NVARCHAR2_COL, CLOB_COL, NCLOB_COL
# --------------------------------------------------

sqlplus -s / as sysdba <<EOF
-- 1) PDB を OPEDB に切り替え
ALTER SESSION SET CONTAINER=OPEDB;
-- 2) Current Schema を ORACLE に切り替え
ALTER SESSION SET CURRENT_SCHEMA=ORACLE;

BEGIN
  EXECUTE IMMEDIATE q'{
    CREATE TABLE CHAR_CLOB_TEST_TABLE (
      ID               NUMBER(10)         NOT NULL,
      CHAR_COL         CHAR(10),
      NCHAR_COL        NCHAR(10),
      VARCHAR2_COL     VARCHAR2(50 CHAR),
      NVARCHAR2_COL    NVARCHAR2(50),
      CLOB_COL         CLOB,
      NCLOB_COL        NCLOB,
      CONSTRAINT PK_CHAR_CLOB_TEST PRIMARY KEY (ID)
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
