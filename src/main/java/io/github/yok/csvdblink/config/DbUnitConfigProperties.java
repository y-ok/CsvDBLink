package io.github.yok.csvdblink.config;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Holds properties applied to DBUnit's {@code DatabaseConfig}.
 *
 * <p>
 * Specify the following properties in {@code application.yml}.
 * </p>
 * <ul>
 * <li>{@code dbunit.config.allow-empty-fields}: Whether empty fields are allowed</li>
 * <li>{@code dbunit.config.batched-statements}: Whether to use batched statement execution</li>
 * <li>{@code dbunit.config.batch-size}: Batch size (number of statements per batch)</li>
 * </ul>
 *
 * @author Yasuharu.Okawauchi
 */
@Component
@ConfigurationProperties(prefix = "dbunit.config")
@Getter
@Setter
@NoArgsConstructor
public class DbUnitConfigProperties {

    /**
     * Specifies whether DBUnit permits empty fields (i.e., {@code ""}).
     */
    private boolean allowEmptyFields = true;

    /**
     * Specifies whether DBUnit's batched statement execution should be enabled.
     */
    private boolean batchedStatements = true;

    /**
     * Specifies the number of statements to execute per batch when DBUnit batching is enabled.
     */
    private int batchSize = 100;
}
