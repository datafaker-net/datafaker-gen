package net.datafaker.datafaker_gen.sink;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.TableName;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class BigQueryConnectionConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Credentials credentials;

    private final String project;
    private final String dataset;
    private final String table;
    private final long maxOutstandingElementsCount;
    private final long maxOutstandingRequestBytes;

    private final boolean createIfNotExists;

    public BigQueryConnectionConfiguration(
            String project,
            String dataset,
            String table,
            boolean createIfNotExists,
            long maxOutstandingElementsCount,
            long maxOutstandingRequestBytes,
            Credentials credentials
    ) {
        this.project = project;
        this.dataset = dataset;
        this.table = table;
        this.createIfNotExists = createIfNotExists;
        this.credentials = credentials;
        this.maxOutstandingElementsCount = maxOutstandingElementsCount;
        this.maxOutstandingRequestBytes = maxOutstandingRequestBytes;
    }

    public static BigQueryConnectionConfiguration fromConfig(Map<String, ?> config) {
        final Credentials credentials;

        String serviceAccount = (String) config.get("service_account");
        try (FileInputStream fis = new FileInputStream(serviceAccount)) {
            if (config.get("test") != null) {
                // Create dummy credentials based on the service account
                credentials = GoogleCredentials.getApplicationDefault();
            } else {
                credentials = ServiceAccountCredentials.fromStream(fis);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        int maxOutstandingElementsCount = (int) config.get("max_outstanding_elements_count");
        int maxOutstandingRequestBytes = (int) config.get("max_outstanding_request_bytes");
        return new BigQueryConnectionConfiguration(
                (String) config.get("project_id"),
                (String) config.get("dataset"),
                (String) config.get("table"),
                (boolean) config.get("create_table_if_not_exists"),
                maxOutstandingElementsCount,
                maxOutstandingRequestBytes,
                credentials
        );
    }

    public TableName getTableName() {
        return TableName.of(project, dataset, table);
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public boolean isCreateIfNotExists() {
        return createIfNotExists;
    }

    public String getProject() {
        return project;
    }

    public long getMaxOutstandingElementsCount() {
        return maxOutstandingElementsCount;
    }

    public long getMaxOutstandingRequestBytes() {
        return maxOutstandingRequestBytes;
    }

}
