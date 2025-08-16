#!/bin/bash
# --------------------------------------------------
# 起動ごとに ORACLE スキーマ下に DATE_TIME_TEST_TABLE を作成（存在すればスキップ）
#   カラム：ID, DATE_COL, TIMESTAMP_COL, TIMESTAMP_TZ_COL,
#           TIMESTAMP_LTZ_COL, INTERVAL_YM_COL, INTERVAL_DS_COL
# --------------------------------------------------

sqlplus -s / as sysdba <<EOF
-- 1) PDB を OPEDB に切り替え
ALTER SESSION SET CONTAINER=OPEDB;
-- 2) Current Schema を ORACLE に切り替え
ALTER SESSION SET CURRENT_SCHEMA=ORACLE;

BEGIN
  EXECUTE IMMEDIATE q'{
    CREATE TABLE DATE_TIME_TEST_TABLE (
      ID                  NUMBER(10)                 NOT NULL,
      DATE_COL            DATE,
      TIMESTAMP_COL       TIMESTAMP,
      TIMESTAMP_TZ_COL    TIMESTAMP WITH TIME ZONE,
      TIMESTAMP_LTZ_COL   TIMESTAMP WITH LOCAL TIME ZONE,
      INTERVAL_YM_COL     INTERVAL YEAR TO MONTH,
      INTERVAL_DS_COL     INTERVAL DAY TO SECOND,
      CONSTRAINT PK_DATE_TIME_TEST PRIMARY KEY (ID)
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
