#!/bin/bash
# --------------------------------------------------
# 起動ごとに ORACLE スキーマ下に NO_PK_TABLE を作成（存在すればスキップ）
#   カラム：ID, CHAR_COL, NCHAR_COL, VARCHAR2_COL, NVARCHAR2_COL,
#          INTERVAL_YM_COL, INTERVAL_DS_COL
# --------------------------------------------------

sqlplus -s / as sysdba <<EOF
-- 1) PDB を OPEDB に切り替え
ALTER SESSION SET CONTAINER=OPEDB;
-- 2) Current Schema を ORACLE に切り替え
ALTER SESSION SET CURRENT_SCHEMA=ORACLE;

BEGIN
  EXECUTE IMMEDIATE q'{
    CREATE TABLE NO_PK_TABLE (
      ID                 NUMBER,
      CHAR_COL           CHAR(10),
      NCHAR_COL          NCHAR(10),
      VARCHAR2_COL       VARCHAR2(100),
      NVARCHAR2_COL      NVARCHAR2(100),
      INTERVAL_YM_COL    INTERVAL YEAR TO MONTH,
      INTERVAL_DS_COL    INTERVAL DAY TO SECOND
    )
  }';
EXCEPTION
  WHEN OTHERS THEN
    -- ORA-00955: name is already used by an existing object
    IF SQLCODE != -955 THEN
      RAISE;
    END IF;
END;
/
EXIT;
EOF
