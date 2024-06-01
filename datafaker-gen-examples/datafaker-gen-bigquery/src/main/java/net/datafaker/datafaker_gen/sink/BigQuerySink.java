package net.datafaker.datafaker_gen.sink;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.TableName;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BigQuerySink implements Sink {
    private static final Logger LOGGER = Logger.getLogger(BigQuerySink.class.getName());

    static {
        Logger rootLogger = Logger.getLogger("");
        rootLogger.setLevel(Level.SEVERE);
        LOGGER.setLevel(Level.INFO);
    }

    @Override
    public String getName() {
        return "bigquery";
    }

    @Override
    public void run(Map<String, ?> config, Function<Integer, ?> function, int numberOfLines) {
        BigQueryConnectionConfiguration configuration = BigQueryConnectionConfiguration.fromConfig(config);

        LOGGER.info(() -> String.format("Running BigQuerySink with %d lines", numberOfLines));

        try {
            BigQueryWriteSettings bigQueryWriteSettings = getBigQueryWriteSettings(configuration);

            BigQueryWriteClient client = BigQueryWriteClient.create(bigQueryWriteSettings);
            ensureTableExists(configuration, function);

            int batchSize = getBatchSize(config);
            BigQueryStreamWriter streamWriter = new BigQueryStreamWriter(configuration, client, function);
            LOGGER.info("Writing to BigQuery...");
            streamWriter.write(batchSize, numberOfLines);
            LOGGER.info("Done writing to BigQuery");
            close(client, streamWriter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void ensureTableExists(BigQueryConnectionConfiguration options, Function<Integer, ?> function) {
        TableName tableName = options.getTableName();
        var bigQueryService =
                BigQueryOptions.newBuilder()
                        .setProjectId(tableName.getProject())
                        .setCredentials(options.getCredentials())
                        .build()
                        .getService();
        var dataset = tableName.getDataset();

        if (bigQueryService.getDataset(dataset) == null
                || !bigQueryService.getDataset(dataset).exists()) {
            bigQueryService.create(DatasetInfo.newBuilder(dataset).build());
        }
        var tableId = TableId.of(tableName.getProject(), tableName.getDataset(), tableName.getTable());
        Table table = bigQueryService.getTable(tableId);

        if (table == null || !table.exists()) {
            if (!options.isCreateIfNotExists()) {
                throw new RuntimeException("Table does not exist, it could be created by setting create_table_if_not_exists to true");
            }
            LOGGER.info("Table does not exist, creating it");

            StandardTableDefinition requiredDefinition = SchemaGenerator.getSchema(function);
            bigQueryService.create(TableInfo.of(tableId, requiredDefinition));
        } else {
            LOGGER.info("Table exists");
        }
    }

    private static BigQueryWriteSettings getBigQueryWriteSettings(BigQueryConnectionConfiguration configuration) throws IOException {
        FixedCredentialsProvider creds = FixedCredentialsProvider.create(configuration.getCredentials());

        BigQueryWriteSettings.Builder bigQueryWriteSettingsBuilder = BigQueryWriteSettings.newBuilder();
        bigQueryWriteSettingsBuilder
                .createWriteStreamSettings()
                .setRetrySettings(
                        bigQueryWriteSettingsBuilder.createWriteStreamSettings().getRetrySettings().toBuilder()
                                .setTotalTimeout(Duration.ofSeconds(30))
                                .build());
        return bigQueryWriteSettingsBuilder.setCredentialsProvider(creds).build();
    }

    public void close(BigQueryWriteClient client, BigQueryStreamWriter streamWriter) throws IOException {
        // streamWriter could fail to init
        if (streamWriter != null) {
            // Close the connection to the server.
            streamWriter.close();
        }

        if (client != null) {
            try {
                if (streamWriter != null) {
                    client.finalizeWriteStream(streamWriter.getStreamName());
                }
            } finally {
                client.close();
            }
        }
    }
}