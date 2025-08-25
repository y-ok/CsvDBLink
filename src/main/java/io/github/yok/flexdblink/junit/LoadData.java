package io.github.yok.flexdblink.junit;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Annotation to automatically load CSV/LOB data at test execution time and roll back after each
 * test.
 *
 * <p>
 * This annotation can be placed on a test class or on individual test methods.<br>
 * Data files are automatically discovered and loaded from:
 * 
 * <pre>
 * src/test/resources/&lt;package-path&gt;/&lt;TestClassName&gt;/&lt;scenario&gt;/&lt;DB name&gt;/
 * </pre>
 * </p>
 *
 * <p>
 * The {@link LoadDataExtension} interprets this annotation and performs the data load.
 * </p>
 *
 * <p>
 * Typical directory example:
 * 
 * <pre>
 * src/test/resources/com/example/project/MyServiceTest/INITIAL/DB1/*.csv
 * src/test/resources/com/example/project/MyServiceTest/COMMON/DB2/*.csv
 * </pre>
 * </p>
 *
 * @author Yasuharu.Okawauchi
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ExtendWith(LoadDataExtension.class)
public @interface LoadData {

    /**
     * Array of target scenario names (directory names). If omitted, all immediate sub-directories
     * under the test class resource root are targeted.
     *
     * @return scenario names (e.g., {@code {"INITIAL", "COMMON"}})
     */
    String[] scenario() default {};

    /**
     * Array of target database names (directory names). If omitted, all sub-directories directly
     * under each scenario directory are targeted.
     *
     * @return database names (e.g., {@code {"DB1", "DB2"}})
     */
    String[] dbNames() default {};
}
