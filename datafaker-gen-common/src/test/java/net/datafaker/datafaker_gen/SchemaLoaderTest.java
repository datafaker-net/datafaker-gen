package net.datafaker.datafaker_gen;

import net.datafaker.transformations.Field;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaLoaderTest {

    @Test
    void loadSchemaFromConfigFile() throws URISyntaxException {
        String resourceName = "./schemas/config_test.yaml";
        String path = Paths.get(Objects.requireNonNull(getClass().getClassLoader().getResource(resourceName)).toURI()).toString();

        Configuration configuration = DatafakerGen.parseArg(new String[]{"-f", "xml", "-n", "10", "-sink", "textfile", "-s", path});
        List<Field> fields = SchemaLoader.getFields(configuration);

        assertThat(fields).hasSize(1);
        assertThat(fields.get(0).getName()).isEqualTo("test");
    }
}