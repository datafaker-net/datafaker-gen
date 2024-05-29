package net.datafaker.datafaker_gen.sink;

import com.google.auth.Credentials;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BigQueryConnectionConfigurationTest {

    @Test
    void testCreateBigQueryConnectionConfigurationFromConfig() {
        BigQueryConnectionConfiguration configuration = BigQueryConnectionConfiguration.fromConfig(getConfig());

        Credentials credentials = configuration.getCredentials();
        assertThat(credentials.getClass().getName()).isEqualTo("com.google.auth.oauth2.ServiceAccountCredentials");
        assertThat(configuration.getProject()).isEqualTo("project_id");
        assertThat(configuration.getTableName().toString()).isEqualTo("projects/project_id/datasets/dataset/tables/table");
        assertThat(configuration.isCreateIfNotExists()).isTrue();

        assertThat(configuration.getMaxOutstandingElementsCount()).isEqualTo(1000);
        assertThat(configuration.getMaxOutstandingRequestBytes()).isEqualTo(1000);
    }

    public static Map<String, Object> getConfig() {
        Map<String, Object> config = new HashMap<>();

        config.put("max_outstanding_elements_count", 1000);
        config.put("max_outstanding_request_bytes", 1000);
        config.put("project_id", "project_id");
        config.put("dataset", "dataset");
        config.put("table", "table");
        config.put("create_table_if_not_exists", true);
        return config;
    }

}