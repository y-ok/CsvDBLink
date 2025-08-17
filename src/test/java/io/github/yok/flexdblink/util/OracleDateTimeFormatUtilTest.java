package io.github.yok.flexdblink.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OracleDateTimeFormatUtilTest {

    private OracleDateTimeFormatUtil util;

    @BeforeEach
    void setup() {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        props.setDate("yyyy-MM-dd");
        props.setTime("HH:mm:ss");
        props.setDateTime("yyyy-MM-dd HH:mm:ss");
        props.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        util = new OracleDateTimeFormatUtil(props);
    }

    @Test
    void formatJdbcDateTime_正常ケース_null入力はnullが返ること() {
        assertNull(util.formatJdbcDateTime("ANY", null, null));
    }

    @Test
    void formatJdbcDateTime_正常ケース_Timestampはミリ秒付きでフォーマットされること() {
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2020, 1, 2, 3, 4, 5, 678_000_000));
        String out = util.formatJdbcDateTime("ANY", ts, null);
        assertEquals("2020-01-02 03:04:05.678", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_Dateは日付フォーマットされること() {
        Date d = Date.valueOf("2020-12-31");
        String out = util.formatJdbcDateTime("ANY", d, null);
        assertEquals("2020-12-31", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_Timeは時刻フォーマットされること() {
        Time t = Time.valueOf("23:59:58");
        String out = util.formatJdbcDateTime("ANY", t, null);
        assertEquals("23:59:58", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_Stringはそのまま返ること() {
        String out = util.formatJdbcDateTime("ANY", "rawstring", null);
        assertEquals("rawstring", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_その他オブジェクトはtoStringが返ること() {
        Object o = new Object() {
            @Override
            public String toString() {
                return "custom";
            }
        };
        String out = util.formatJdbcDateTime("ANY", o, null);
        assertEquals("custom", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_colNameがTIMESTAMP_LTZ_COLでタイムゾーン削除されること() {
        String in = "2020-01-01 10:00:00 Asia/Tokyo";
        String out = util.formatJdbcDateTime("TIMESTAMP_LTZ_COL", in, null);
        assertEquals("2020-01-01 10:00:00", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_colNameがTIMESTAMP_COLで小数部削除されること() {
        String in = "2020-01-01 10:00:00.0";
        String out = util.formatJdbcDateTime("TIMESTAMP_COL", in, null);
        assertEquals("2020-01-01 10:00:00", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_colNameがDATE_COLで小数部削除されること() {
        String in = "2020-01-01.0";
        String out = util.formatJdbcDateTime("DATE_COL", in, null);
        assertEquals("2020-01-01", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_colNameがINTERVAL_YM_COLで正規化されること() {
        String in = "2-3";
        String out = util.formatJdbcDateTime("INTERVAL_YM_COL", in, null);
        assertEquals("+02-03", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_colNameがINTERVAL_DS_COLで正規化されること() {
        String in = "2 3:4:5.0";
        String out = util.formatJdbcDateTime("INTERVAL_DS_COL", in, null);
        assertEquals("+02 03:04:05", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_未知の列名は変化しないこと() {
        String in = "nochange";
        String out = util.formatJdbcDateTime("OTHER_COL", in, null);
        assertEquals("nochange", out);
    }

    @Test
    void formatJdbcDateTime_異常ケース_stringValue実行時例外でtoStringが返ること() {
        class BadTstz {
            @Override
            public String toString() {
                return "fallback";
            }

            @SuppressWarnings("unused")
            public String stringValue(Connection c) {
                throw new RuntimeException("boom");
            }
        }

        String out = util.formatJdbcDateTime("ANY", new BadTstz(), null);

        assertEquals("fallback", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_TimestampTz_null入力はnullが返ること() {
        String out = util.formatJdbcDateTime("ANY", (String) null, null);
        assertNull(out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_TimestampTz_リージョン異常は元の文字列が返ること() {
        String in = "2020-01-01 12:00:00 Not/AZone";
        String out = util.formatJdbcDateTime("ANY", in, null);
        assertEquals("2020-01-01 12:00:00 Not/AZone", out);
    }

    @Test
    void formatJdbcDateTime_正常ケース_TimestampCol_小数部付きは小数が削除されること() {
        String in = "2020-01-01 12:34:56.0";
        String out = util.formatJdbcDateTime("TIMESTAMP_COL", in, null);
        assertEquals("2020-01-01 12:34:56", out);
    }
}
