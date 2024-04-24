package net.datafaker.datafaker_gen;

import net.datafaker.Faker;
import org.junit.jupiter.api.Test;

public class DatafakerIssuesTest {
    @Test
    void issue1168() {
       (new Faker()).idNumber().valid();
       (new Faker()).regexify("[a-z]{6}");
    }
}
