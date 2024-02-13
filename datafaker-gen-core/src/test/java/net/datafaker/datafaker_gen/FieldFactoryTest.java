package net.datafaker.datafaker_gen;

import net.datafaker.Faker;
import net.datafaker.service.RandomService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FieldFactoryTest {

    @Test
    void shouldGenerateFieldWithSameLocal() {
        var defaultLocale = Locale.forLanguageTag(Locale.ENGLISH.toLanguageTag());
        Faker faker = new Faker(defaultLocale, new RandomService(new Random(1)));

        List<Supplier<?>> generators = FieldFactory.getInstance().getGenerators(faker, List.of("Name#firstName", "Name#lastName"), defaultLocale, defaultLocale);

        assertEquals(2, generators.size());
        assertEquals("Darrel", generators.get(0).get());
        assertEquals("Bogisich", generators.get(1).get());
    }

    @Test
    void shouldGenerateFieldFromCustomObject() {
        var defaultLocale = Locale.forLanguageTag(Locale.ENGLISH.toLanguageTag());
        Faker faker = new Faker(defaultLocale, new RandomService(new Random(1)));

        List<Supplier<?>> generators = FieldFactory.getInstance().getGenerators(faker, List.of("net.not.datafaker.DefaultPerson#getName"), defaultLocale, defaultLocale);

        assertEquals(1, generators.size());
        assertEquals("Value", generators.get(0).get());
    }
}