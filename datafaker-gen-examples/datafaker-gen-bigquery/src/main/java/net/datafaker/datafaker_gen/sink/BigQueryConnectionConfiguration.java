package net.datafaker.datafaker_gen.sink;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.TableName;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public class BigQueryConnectionConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final int KEEP_ALIVE_TIMEOUT_IN_SECONDS_DEFAULT = 60;
    public static final int KEEP_ALIVE_TIME_IN_SECONDS_DEFAULT = 60;
    public static final String SERVICE_ACCOUNT_SECRET = "SERVICE_ACCOUNT_SECRET";
    public static final String SERVICE_ACCOUNT_PROPERTY = "service_account";
    private final Credentials credentials;

    private final String project;
    private final String dataset;
    private final String table;
    private final long maxOutstandingElementsCount;
    private final long maxOutstandingRequestBytes;

    private final boolean createIfNotExists;
    private final long keepAliveTimeInSeconds;
    private final long keepAliveTimeoutInSeconds;

    public BigQueryConnectionConfiguration(
            String project,
            String dataset,
            String table,
            boolean createIfNotExists,
            long maxOutstandingElementsCount,
            long maxOutstandingRequestBytes,
            long keepAliveTimeInSeconds,
            long keepAliveTimeoutInSeconds,
            Credentials credentials
    ) {
        this.project = project;
        this.dataset = dataset;
        this.table = table;
        this.createIfNotExists = createIfNotExists;
        this.credentials = credentials;
        this.maxOutstandingElementsCount = maxOutstandingElementsCount;
        this.maxOutstandingRequestBytes = maxOutstandingRequestBytes;
        this.keepAliveTimeInSeconds = keepAliveTimeInSeconds;
        this.keepAliveTimeoutInSeconds = keepAliveTimeoutInSeconds;
    }

    public static BigQueryConnectionConfiguration fromConfig(Map<String, ?> config) {
        final Credentials credentials = getCredentials(config);

        int maxOutstandingElementsCount = (int) config.get("max_outstanding_elements_count");
        int maxOutstandingRequestBytes = (int) config.get("max_outstanding_request_bytes");
        int keepAliveTimeInSeconds = Optional.ofNullable(config.get("keep_alive_time_in_seconds")).map(v -> (int) v).orElse(KEEP_ALIVE_TIME_IN_SECONDS_DEFAULT);
        int keepAliveTimeoutInSeconds = Optional.ofNullable(config.get("keep_alive_timeout_in_seconds")).map(v -> (int) v).orElse(KEEP_ALIVE_TIMEOUT_IN_SECONDS_DEFAULT);
        return new BigQueryConnectionConfiguration(
                (String) config.get("project_id"),
                (String) config.get("dataset"),
                (String) config.get("table"),
                (boolean) config.get("create_table_if_not_exists"),
                maxOutstandingElementsCount,
                maxOutstandingRequestBytes,
                keepAliveTimeInSeconds,
                keepAliveTimeoutInSeconds,
                credentials
        );
    }

    private static Credentials getCredentials(Map<String, ?> config) {
        final Credentials credentials;

        try {
            String serviceAccountSecretContent = System.getenv(SERVICE_ACCOUNT_SECRET);
            if (serviceAccountSecretContent != null) {
                credentials = ServiceAccountCredentials.fromStream(new BufferedInputStream(new ByteArrayInputStream(serviceAccountSecretContent.getBytes())));
            } else {
                String serviceAccount = (String) config.get(SERVICE_ACCOUNT_PROPERTY);
                if (serviceAccount != null) {
                    try (FileInputStream fis = new FileInputStream(serviceAccount)) {
                        credentials = ServiceAccountCredentials.fromStream(fis);
                    }
                } else {
                    credentials = GoogleCredentials.getApplicationDefault();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return credentials;
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

    public long getKeepAliveTimeInSeconds() {
        return keepAliveTimeInSeconds;
    }

    public long getKeepAliveTimeoutInSeconds() {
        return keepAliveTimeoutInSeconds;
    }

    public long getMaxOutstandingRequestBytes() {
        return maxOutstandingRequestBytes;
    }

}
