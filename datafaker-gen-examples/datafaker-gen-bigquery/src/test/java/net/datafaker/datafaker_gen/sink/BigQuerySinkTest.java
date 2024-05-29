package net.datafaker.datafaker_gen.sink;

import org.junit.jupiter.api.Test;

import static net.datafaker.datafaker_gen.sink.BigQueryConnectionConfigurationTest.getConfig;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigQuerySinkTest {

    @Test
    void testRunWhenWrongConfiguration() {
        BigQuerySink bigQuerySink = new BigQuerySink();
        assertThatThrownBy(() -> bigQuerySink.run(getConfig(), null, 0))
                .isInstanceOf(RuntimeException.class);
    }
}