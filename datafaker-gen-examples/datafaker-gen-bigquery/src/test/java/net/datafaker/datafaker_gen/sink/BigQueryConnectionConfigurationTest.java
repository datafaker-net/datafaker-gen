package net.datafaker.datafaker_gen.sink;


import com.google.auth.Credentials;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BigQueryConnectionConfigurationTest {

    @Test
    void testCreateBigQueryConnectionConfigurationFromConfig() {
        String serviceAccount = getClass().getClassLoader().getResource("test_service_account.json").getPath();
        BigQueryConnectionConfiguration configuration = BigQueryConnectionConfiguration.fromConfig(getConfig(serviceAccount));

        Credentials credentials = configuration.getCredentials();
        assertEquals(credentials.getClass().getName(), "com.google.auth.oauth2.UserCredentials");
        assertEquals("project_id", configuration.getProject());
        assertEquals("projects/project_id/datasets/dataset/tables/table", configuration.getTableName().toString());
        assertTrue(configuration.isCreateIfNotExists());
        assertEquals(1000, configuration.getMaxOutstandingElementsCount());
        assertEquals(1000, configuration.getMaxOutstandingRequestBytes());
    }

    public static Map<String, Object> getConfig(String serviceAccount) {
        Map<String, Object> config = new HashMap<>();

        // Create dummy credentials based on the service account
        config.put("test", true);
        config.put("service_account", serviceAccount);

        config.put("max_outstanding_elements_count", 1000);
        config.put("max_outstanding_request_bytes", 1000);
        config.put("project_id", "project_id");
        config.put("dataset", "dataset");
        config.put("table", "table");
        config.put("create_table_if_not_exists", true);
        return config;
    }

}