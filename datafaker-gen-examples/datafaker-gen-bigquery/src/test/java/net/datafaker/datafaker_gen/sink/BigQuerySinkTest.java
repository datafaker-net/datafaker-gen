package net.datafaker.datafaker_gen.sink;

import org.junit.jupiter.api.Test;

import static net.datafaker.datafaker_gen.sink.BigQueryConnectionConfigurationTest.getConfig;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BigQuerySinkTest {

    @Test
    void testRunWhenWrongConfiguration() {
        String serviceAccount = getClass().getClassLoader().getResource("test_service_account.json").getPath();

        BigQuerySink bigQuerySink = new BigQuerySink();
        assertThrows(RuntimeException.class, () -> {
            bigQuerySink.run(getConfig(serviceAccount), null, 0);
        });
    }
}