#!/bin/bash
# --------------------------------------------------
# ファイル名：create_varchar2_char_test_table.sh
# 概要：起動ごとに ORACLE スキーマ下に
#       VARCHAR2_CHAR_TEST_TABLE を作成（存在すればスキップ）
#       LOB／バイナリ型を除外し、基本データ型を網羅
# --------------------------------------------------

sqlplus -s / as sysdba <<'EOF'
-- 1) PDB を OPEDB に切り替え
ALTER SESSION SET CONTAINER=OPEDB;

-- 2) Current Schema を ORACLE に切り替え
ALTER SESSION SET CURRENT_SCHEMA=ORACLE;

-- PL/SQL ブロック開始
BEGIN
  EXECUTE IMMEDIATE '
    CREATE TABLE VARCHAR2_CHAR_TEST_TABLE (
      -- プライマリキー列
      ID                          NUMBER(10)         NOT NULL,

      -- 文字列型
      VARCHAR2_6CHAR_COL          VARCHAR2(6 CHAR),
      VARCHAR2_6BYTE_COL          VARCHAR2(6 BYTE),
      CHAR_6CHAR_COL              CHAR(6 CHAR),
      NCHAR_6CHAR_COL             NCHAR(6),
      NVARCHAR2_6CHAR_COL         NVARCHAR2(6),

      -- 数値型
      NUMBER_8_2_COL              NUMBER(8,2),
      BINARY_FLOAT_COL            BINARY_FLOAT,
      BINARY_DOUBLE_COL           BINARY_DOUBLE,

      -- 日時型
      DATE_COL                    DATE,
      TIMESTAMP_COL               TIMESTAMP,
      TIMESTAMP_TZ_COL            TIMESTAMP WITH TIME ZONE,
      TIMESTAMP_LTZ_COL           TIMESTAMP WITH LOCAL TIME ZONE,
      INTERVAL_YM_COL             INTERVAL YEAR TO MONTH,
      INTERVAL_DS_COL             INTERVAL DAY TO SECOND,

      -- プライマリキー制約
      CONSTRAINT PK_VARCHAR2_CHAR_TEST PRIMARY KEY (ID)
    )
  ';
EXCEPTION
  WHEN OTHERS THEN
    -- ORA-00955: 既存オブジェクトがある場合のみ無視
    IF SQLCODE != -955 THEN
      RAISE;
    END IF;
END;
/
-- SQL*Plus セッション終了
EXIT;
EOF
