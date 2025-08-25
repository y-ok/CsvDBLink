package io.github.yok.flexdblink.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tells the JUnit 5 extension to compare the database state against expected dataset files under
 * {@code expected/} after the test finishes.
 *
 * <p>
 * Expected files are placed per DB as: {@code ${pathsConfig.load}/<scenario>/expected/<DB name>/
 * <table>
 * .(csv|json|yaml|yml|xml)}
 * </p>
 *
 * <p>
 * If any difference is found, an {@link AssertionError} is thrown.
 * </p>
 *
 * <pre>
 * {@code
 * &#64;Test
 * &#64;LoadData("scenarioA")
 * &#64;ExpectedData(scenario = "scenarioA", excludeColumns = {"UPDATED_AT"})
 * void testUserInsert() { ... }
 * }
 * </pre>
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(ExpectedDataExtension.class)
public @interface ExpectedData {

    /**
     * Scenario directory name under {@code ${pathsConfig.load}}. If empty, the test class simple
     * name is used. The directory must contain {@code expected/}.
     *
     * @return scenario name (empty allowed)
     */
    String scenario() default "";

    /**
     * Array of target database names (directory names). If omitted, all sub-directories directly
     * under each scenario directory are targeted.
     *
     * @return database names (e.g., {@code {"DB1", "DB2"}})
     */
    String[] dbNames() default {};

    /**
     * Column names to exclude from comparison. Useful for auto-generated fields (timestamps,
     * versions, etc.). Prefer UPPER_CASE.
     *
     * @return columns to ignore
     */
    String[] excludeColumns() default {};
}
