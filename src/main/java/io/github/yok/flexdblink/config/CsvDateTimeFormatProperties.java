package io.github.yok.flexdblink.config;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Property class that holds date/time format patterns used when writing CSV files.
 *
 * <p>
 * Specify the following properties in {@code application.yml} or {@code application.properties}.
 * </p>
 * <ul>
 * <li>{@code dbunit.csv.format.date}: Date format (e.g., {@code yyyy-MM-dd})</li>
 * <li>{@code dbunit.csv.format.time}: Time format (e.g., {@code HH:mm:ss})</li>
 * <li>{@code dbunit.csv.format.dateTime}: Date-time format (e.g., {@code yyyy-MM-dd HH:mm:ss})</li>
 * <li>{@code dbunit.csv.format.dateTimeWithMillis}: Date-time format with milliseconds (e.g.,
 * {@code yyyy-MM-dd HH:mm:ss.SSS})</li>
 * </ul>
 *
 * <p>
 * If these properties are not provided, no format definitions will be available and errors may
 * occur at usage time.
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Component
@ConfigurationProperties(prefix = "dbunit.csv.format")
@Getter
@Setter
@NoArgsConstructor
public class CsvDateTimeFormatProperties {

    /**
     * Pattern string for Date values.
     *
     * <p>
     * Example: {@code yyyy-MM-dd}
     * </p>
     */
    private String date;

    /**
     * Pattern string for Time values.
     *
     * <p>
     * Example: {@code HH:mm:ss}
     * </p>
     */
    private String time;

    /**
     * Pattern string for Timestamp (date-time) values.
     *
     * <p>
     * Example: {@code yyyy-MM-dd HH:mm:ss}
     * </p>
     */
    private String dateTime;

    /**
     * Pattern string for Timestamp values with milliseconds.
     *
     * <p>
     * Example: {@code yyyy-MM-dd HH:mm:ss.SSS}
     * </p>
     */
    private String dateTimeWithMillis;

}
