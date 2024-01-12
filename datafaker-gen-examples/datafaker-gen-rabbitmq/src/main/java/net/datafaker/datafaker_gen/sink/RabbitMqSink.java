package net.datafaker.datafaker_gen.sink;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

public class RabbitMqSink implements Sink {

    @Override
    public String getName() {
        return "rabbitmq";
    }

    @Override
    public void run(Map<String, ?> config, Function<Integer, ?> function, int numberOfLines) {
        int numberOfLinesToPrint = numberOfLines;
        String host = (String) config.get("host");
        int port = (int) config.get("port");
        String username = (String) config.get("username");
        String password = (String) config.get("password");
        String exchange = (String) config.get("exchange");
        String routingKey = (String) config.get("routingkey");
        int batchSize = getBatchSize(config);

        ConnectionFactory factory = getConnectionFactory(host, port, username, password);
        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();
            if (batchSize > 1) {
                System.out.println("Batch size is greater than 1, sending " + numberOfLines/batchSize + " messages");
                publishBatch(function, batchSize, numberOfLinesToPrint, channel, exchange, routingKey);
            } else {
                String lines = (String) function.apply(numberOfLinesToPrint);
                JsonArray jsonArray = JsonParser.parseString(lines).getAsJsonArray();

                System.out.println("Batch size is 1, sending " + jsonArray.size() + " messages");
                jsonArray.forEach(jsonElement -> {
                    try {
                        channel.basicPublish(exchange, routingKey, null, jsonElement.toString().getBytes());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void publishBatch(Function<Integer, ?> function, int batchSize, int numberOfLinesToPrint, Channel channel, String exchange, String routingKey) throws IOException {

        while (numberOfLinesToPrint > 0) {
            var numberOfLinesToPrintCurrentIteration = Math.min(batchSize, numberOfLinesToPrint);
            String lines = (String) function.apply(numberOfLinesToPrintCurrentIteration);
            if (numberOfLinesToPrintCurrentIteration == 1) {
                lines = "[" + lines + "]";
            }
            JsonArray jsonArray = JsonParser.parseString(lines).getAsJsonArray();

            StringBuilder batch = new StringBuilder();
            batch.append("[");
            int count = 0;
            int total = jsonArray.size();

            for (JsonElement jsonElement : jsonArray) {
                batch.append(jsonElement.toString());
                count++;
                total--;
                if (count != batchSize && total != 0){
                    batch.append(",");
                }

                if (count == batchSize) {
                    batch.append("]");
                    channel.basicPublish(exchange, routingKey, null, batch.toString().getBytes());
                    batch = new StringBuilder();
                    batch.append("[");
                    count = 0;
                }
            }

            if (batch.length() > 1) {
                batch.append("]");
                channel.basicPublish(exchange, routingKey, null, batch.toString().getBytes());
            }

            numberOfLinesToPrint -= numberOfLinesToPrintCurrentIteration;
        }

    }

    private static ConnectionFactory getConnectionFactory(String host, int port, String username, String password) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        return factory;
    }

}