package net.datafaker.datafaker_gen.sink;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors;
import io.grpc.Status;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.grpc.Status.Code.ALREADY_EXISTS;

public class BigQueryStreamWriter {
    private final Function<Integer, ?> function;
    private final JsonStreamWriter streamWriter;

    public BigQueryStreamWriter(BigQueryConnectionConfiguration configuration, BigQueryWriteClient client, Function<Integer, ?> function) {
        this.function = function;
        try {
            this.streamWriter = getStreamWriter(configuration, client);
        } catch (IOException | Descriptors.DescriptorValidationException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected JsonStreamWriter getStreamWriter(
            BigQueryConnectionConfiguration configuration, BigQueryWriteClient client)
            throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();

        CreateWriteStreamRequest createWriteStreamRequest =
                CreateWriteStreamRequest.newBuilder()
                        .setParent(configuration.getTableName().toString())
                        .setWriteStream(stream)
                        .build();
        WriteStream writeStream = client.createWriteStream(createWriteStreamRequest);

        JsonStreamWriter.Builder builder =
                JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema(), client)
                        .setFlowControlSettings(
                                FlowControlSettings.newBuilder()
                                        .setMaxOutstandingElementCount(configuration.getMaxOutstandingElementsCount())
                                        .setMaxOutstandingRequestBytes(configuration.getMaxOutstandingRequestBytes())
                                        .build());
        return builder
                .setExecutorProvider(FixedExecutorProvider.create(Executors.newScheduledThreadPool(100)))
                .setChannelProvider(
                        BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                                .setKeepAliveTime(org.threeten.bp.Duration.ofSeconds(configuration.getKeepAliveTimeInSeconds()))
                                .setKeepAliveTimeout(org.threeten.bp.Duration.ofSeconds(configuration.getKeepAliveTimeoutInSeconds()))
                                .setKeepAliveWithoutCalls(true)
                                // .setChannelsPerCpu(2)
                                .build())
                .build();
    }

    public void write(int batchSize, int numberOfLines) throws Descriptors.DescriptorValidationException, IOException {
        if (numberOfLines > 1) {
            writeMultipleRecords(batchSize, numberOfLines);
        } else {
            writeSingleRecord();
        }
    }

    private void writeMultipleRecords(int batchSize, int numberOfLines) throws Descriptors.DescriptorValidationException, IOException {
        if (batchSize > 1) {
            int i;
            for (i = 0; i < numberOfLines; i++) {
                int currentChunk = Math.min(batchSize, numberOfLines - i);
                String line = (String) function.apply(currentChunk);
                i += batchSize - 1;
                JSONArray arr = new JSONArray(line);
                append(arr, streamWriter);
            }
        } else {
            for(int i = 0; i < numberOfLines; i++) {
                writeSingleRecord();
            }
        }
    }

    private void writeSingleRecord() throws Descriptors.DescriptorValidationException, IOException {
        JSONArray arr = new JSONArray();
        String line = (String) function.apply(1);
        arr.put(new JSONObject(line));
        append(arr, streamWriter);
    }

    protected void append(JSONArray data, JsonStreamWriter streamWriter) throws Descriptors.DescriptorValidationException, IOException {
        ApiFuture<AppendRowsResponse> future = streamWriter.append(data);
        ApiFutures.addCallback(
                future,
                new ApiFutureCallback<>() {
                    @Override
                    public void onFailure(Throwable throwable) {
                        Status status = Status.fromThrowable(throwable);

                        if (status.getCode() == ALREADY_EXISTS) {
                            System.out.printf("Message for this offset already exists");
                            return;
                        }

                        if (throwable instanceof Exceptions.AppendSerializationError) {
                            Exceptions.AppendSerializationError ase = (Exceptions.AppendSerializationError) throwable;
                            Map<Integer, String> rowIndexToErrorMessage = ase.getRowIndexToErrorMessage();
                            if (!rowIndexToErrorMessage.isEmpty()) {
                                // Omit the faulty rows
                                JSONArray dataNew = new JSONArray();
                                for (int i = 0; i < data.length(); i++) {
                                    if (!rowIndexToErrorMessage.containsKey(i)) {
                                        dataNew.put(data.get(i));
                                    } else {
                                        // process faulty rows by placing them on a dead-letter-queue, for instance
                                    }
                                }

                            }
                        }
                    }

                    @Override
                    public void onSuccess(AppendRowsResponse result) {
                        // Do nothing
                    }
                },
                MoreExecutors.directExecutor());

    }

    public void close() {
        streamWriter.close();
    }

    public String getStreamName() {
        return streamWriter.getStreamName();
    }
}
