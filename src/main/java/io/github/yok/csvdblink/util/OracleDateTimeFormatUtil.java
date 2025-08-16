package io.github.yok.csvdblink.util;

import io.github.yok.csvdblink.config.CsvDateTimeFormatProperties;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Utility for normalizing Oracle JDBC date/time/interval values into CSV-friendly strings.
 *
 * <p>
 * This class focuses on values retrieved through the Oracle JDBC driver (e.g.,
 * {@code oracle.sql.TIMESTAMPTZ}, {@code oracle.sql.TIMESTAMPLTZ}) and standard JDBC types
 * ({@link java.sql.Date}, {@link java.sql.Time}, {@link java.sql.Timestamp}). It formats them into
 * stable, locale-independent strings suitable for deterministic CSV exports.
 * </p>
 *
 * <p>
 * Formatting patterns are provided via {@link CsvDateTimeFormatProperties} at construction time.
 * The class is stateless and thread-safe once constructed.
 * </p>
 *
 * <h3>Special handling</h3>
 * <ul>
 * <li><strong>TIMESTAMPTZ / TIMESTAMPLTZ</strong>: uses the driver's
 * {@code stringValue(Connection)} and normalizes zone suffixes to {@code +HHmm} (removing a colon)
 * or resolves region IDs (e.g. {@code Asia/Tokyo}) to a fixed numeric offset at the given
 * instant.</li>
 * <li><strong>INTERVAL YEAR TO MONTH</strong>: normalized to {@code [+|-]YY-MM} (zero-padded).</li>
 * <li><strong>INTERVAL DAY TO SECOND</strong>: normalized to {@code [+|-]DD HH:MM:SS} (zero-padded;
 * fractional seconds dropped).</li>
 * <li>Trailing redundant fractional part like {@code .0} is removed where appropriate.</li>
 * </ul>
 *
 * <p>
 * Column-specific tweaks are triggered by column names. For example, {@code TIMESTAMP_LTZ_COL}
 * drops timezone/region tails, while {@code TIMESTAMP_COL} and {@code DATE_COL} drop redundant
 * trailing {@code .0}. These names are illustrative and can be adapted to your schema conventions.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 * @since 1.0
 */
@Slf4j
@Component
public class OracleDateTimeFormatUtil {

    // Date format (e.g. {@code yyyy-MM-dd})
    private final DateTimeFormatter dateFormatter;
    // Time format (e.g. {@code HH:mm:ss})
    private final DateTimeFormatter timeFormatter;
    // Timestamp format with milliseconds (e.g. {@code yyyy-MM-dd HH:mm:ss.SSS})
    private final DateTimeFormatter dateTimeMillisFormatter;
    // Timestamp format without milliseconds (e.g. {@code yyyy-MM-dd HH:mm:ss})
    private final DateTimeFormatter dateTimeFormatter;

    /**
     * Creates a new formatter using the provided CSV date/time patterns.
     *
     * @param props format patterns for date, time, timestamp(with/without millis)
     * @throws IllegalArgumentException if any pattern is invalid
     */
    public OracleDateTimeFormatUtil(CsvDateTimeFormatProperties props) {
        this.dateFormatter = DateTimeFormatter.ofPattern(props.getDate());
        this.timeFormatter = DateTimeFormatter.ofPattern(props.getTime());
        this.dateTimeMillisFormatter = DateTimeFormatter.ofPattern(props.getDateTimeWithMillis());
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(props.getDateTime());
    }

    /**
     * Formats a JDBC value into a CSV-friendly string using column-aware normalization.
     *
     * <p>
     * Behaviors:
     * </p>
     * <ul>
     * <li>{@code oracle.sql.TIMESTAMPTZ}/{@code TIMESTAMPLTZ} (or their {@code oracle.jdbc.*}
     * cousins): resolve to a string via {@code stringValue(Connection)} and normalize timezone
     * suffix.</li>
     * <li>{@link Timestamp}: formatted with {@link #dateTimeMillisFormatter}.</li>
     * <li>{@link Date}: formatted with {@link #dateFormatter}.</li>
     * <li>{@link Time}: formatted with {@link #timeFormatter}.</li>
     * <li>{@link String}: returned as is (then subject to column-specific cleanup).</li>
     * <li>Other objects: {@code toString()} fallback.</li>
     * </ul>
     *
     * <p>
     * Column-specific post-processing:
     * </p>
     * <ul>
     * <li><code>TIMESTAMP_LTZ_COL</code>: drop region/zone tail and colon in offset; remove
     * trailing <code>.0</code>.</li>
     * <li><code>TIMESTAMP_COL</code>, <code>DATE_COL</code>: remove trailing <code>.0</code>.</li>
     * <li><code>INTERVAL_YM_COL</code>: normalize using {@link #normalizeIntervalYm(String)}.</li>
     * <li><code>INTERVAL_DS_COL</code>: normalize using {@link #normalizeIntervalDs(String)}.</li>
     * </ul>
     *
     * @param colName column name (case-insensitive; used for special handling)
     * @param value JDBC value retrieved from Oracle
     * @param conn JDBC connection used only when the Oracle type requires it (e.g. TIMESTAMPTZ
     *        stringValue)
     * @return normalized string for CSV output, or {@code null} if {@code value} is {@code null}
     */
    public String formatJdbcDateTime(String colName, Object value, Connection conn) {
        if (value == null) {
            return null;
        }

        String normalized;
        try {
            String className = value.getClass().getName();
            if ("oracle.sql.TIMESTAMPTZ".equals(className)
                    || "oracle.jdbc.OracleTIMESTAMPTZ".equals(className)
                    || "oracle.sql.TIMESTAMPLTZ".equals(className)
                    || "oracle.jdbc.OracleTimestampltz".equals(className)) {

                // Call TIMESTAMPTZ/TIMESTAMPLTZ#stringValue(Connection) reflectively to avoid hard
                // dependency
                Method m = value.getClass().getMethod("stringValue", Connection.class);
                String raw = (String) m.invoke(value, conn);
                normalized = normalizeTimestampTz(raw);

            } else if (value instanceof Timestamp) {
                normalized = ((Timestamp) value).toLocalDateTime().format(dateTimeMillisFormatter);

            } else if (value instanceof Date) {
                normalized = ((Date) value).toLocalDate().format(dateFormatter);

            } else if (value instanceof Time) {
                normalized = ((Time) value).toLocalTime().format(timeFormatter);

            } else if (value instanceof String) {
                normalized = (String) value;

            } else {
                normalized = value.toString();
            }

            // Column-specific cleanup/normalization
            switch (colName.toUpperCase()) {
                case "TIMESTAMP_LTZ_COL":
                    normalized = normalized.replaceAll(" [A-Za-z0-9/_+\\-]+$", "")
                            .replaceAll("([+-]\\d{2}):(\\d{2})$", "") // remove colon in +HH:MM
                            .replaceAll("\\.0+$", ""); // trim redundant .0
                    break;
                case "TIMESTAMP_COL":
                case "DATE_COL":
                    normalized = normalized.replaceAll("\\.0+$", "");
                    break;
                case "INTERVAL_YM_COL":
                    normalized = normalizeIntervalYm(normalized);
                    break;
                case "INTERVAL_DS_COL":
                    normalized = normalizeIntervalDs(normalized);
                    break;
                default:
                    // e.g. TIMESTAMP_TZ_COL — keep as produced above
                    break;
            }

            return normalized;

        } catch (Exception ex) {
            log.warn("Failed to normalize Oracle temporal value: colName={}, value={}", colName,
                    value, ex);
            return value.toString();
        }
    }

    /**
     * Normalizes an Oracle TIMESTAMPTZ/TIMESTAMPLTZ string.
     *
     * <ul>
     * <li>If the suffix is a numeric offset {@code +HH:MM}, convert to {@code +HHMM}.</li>
     * <li>If the suffix is a region ID (e.g., {@code Area/Location}), resolve it at the given
     * instant to a numeric offset and render as {@code +HHMM}.</li>
     * <li>Strip redundant trailing {@code .0}.</li>
     * </ul>
     *
     * @param raw raw string from {@code stringValue(Connection)}
     * @return normalized string (never null unless input is null)
     */
    private String normalizeTimestampTz(String raw) {
        if (raw == null) {
            return null;
        }
        String s = raw.trim();
        // yyyy-MM-dd HH:mm:ss[.fraction] +HH:MM -> yyyy-MM-dd HH:mm:ss +HHMM
        Matcher m1 = Pattern.compile(
                "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})(?:\\.\\d+)? ([+-]\\d{2}):(\\d{2})$")
                .matcher(s);
        if (m1.matches()) {
            return m1.group(1) + " " + m1.group(2) + m1.group(3);
        }
        // yyyy-MM-dd HH:mm:ss[.fraction] Region/Zone -> resolve to +HHMM
        Matcher m2 = Pattern.compile(
                "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})(?:\\.\\d+)? ([A-Za-z_]+/[A-Za-z_]+)$")
                .matcher(s);
        if (m2.matches()) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(m2.group(1), dateTimeFormatter);
                String offset =
                        ldt.atZone(ZoneId.of(m2.group(2))).getOffset().getId().replace(":", "");
                return m2.group(1) + " " + offset;
            } catch (DateTimeParseException e) {
                log.warn("Failed to resolve region offset in TIMESTAMPTZ: raw='{}'", raw, e);
                return s;
            }
        }
        return s.replaceAll("\\.0+$", "");
    }

    /**
     * Normalizes INTERVAL YEAR TO MONTH to {@code [+|-]YY-MM} with zero-padding.
     *
     * @param input raw interval string (e.g., {@code "1-6"}, {@code "-2-03"})
     * @return normalized interval string; returns {@code input} unchanged if it doesn't match the
     *         pattern
     */
    private String normalizeIntervalYm(String input) {
        String s = input.replaceAll("[\\s\u3000]+", "");
        Matcher m = Pattern.compile("^(-?)(\\d+)-(\\d+)$").matcher(s);
        if (m.matches()) {
            String sign = m.group(1).isEmpty() ? "+" : "-";
            int years = Integer.parseInt(m.group(2));
            int months = Integer.parseInt(m.group(3));
            return String.format("%s%02d-%02d", sign, years, months);
        }
        return input;
    }

    /**
     * Normalizes INTERVAL DAY TO SECOND to {@code [+|-]DD HH:MM:SS} with zero-padding and without
     * fractional seconds.
     *
     * @param input raw interval string (e.g., {@code "0 5:0:0.0"}, {@code "-2 10:02:03"})
     * @return normalized interval string; returns {@code input} unchanged if it doesn't match the
     *         pattern
     */
    private String normalizeIntervalDs(String input) {
        String s = input.replaceAll("\\.\\d+$", "").replaceAll("[\\s\u3000]+", " ");
        Matcher m = Pattern.compile("^(-?)(\\d+) (\\d+):(\\d+):(\\d+)$").matcher(s);
        if (m.matches()) {
            String sign = m.group(1).isEmpty() ? "+" : "-";
            int days = Integer.parseInt(m.group(2));
            int hours = Integer.parseInt(m.group(3));
            int minutes = Integer.parseInt(m.group(4));
            int seconds = Integer.parseInt(m.group(5));
            return String.format("%s%02d %02d:%02d:%02d", sign, days, hours, minutes, seconds);
        }
        return input;
    }
}
