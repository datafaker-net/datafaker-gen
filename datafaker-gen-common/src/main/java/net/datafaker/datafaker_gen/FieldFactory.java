package net.datafaker.datafaker_gen;

import net.datafaker.Faker;
import net.datafaker.transformations.Field;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static net.datafaker.transformations.Field.field;

public class FieldFactory {

    private static final Map<Class, Map<String, Method>> CLASS2NAME2METHOD = new IdentityHashMap<>();

    private FieldFactory() {
    }

    private static class FieldFactoryHolder {
        private static final FieldFactory INSTANCE = new FieldFactory();
    }

    public static FieldFactory getInstance() {
        return FieldFactoryHolder.INSTANCE;
    }

    public <T> T get(Faker faker, Map<String, Object> object, Locale defaultLocale) {
        final FieldType type = FieldType.validateRequiredFieldsAndGet(object);
        final String name = (String) object.get("name");
        final Locale fieldLocale = parseLocale((String) object.get("locale"), defaultLocale);
        final List<Supplier<?>> generators = getGenerators(faker, (List<String>) object.get("generators"), fieldLocale, defaultLocale);
        final Object nullRateObj = object.get("nullRate");
        final double nullRate = parseNullRate(nullRateObj);

        switch (type) {
            case ARRAY:
                Object minLengthObj = object.get("minLength");
                int minLength = minLengthObj instanceof Number
                        ? ((Number) minLengthObj).intValue()
                        : Integer.parseInt(String.valueOf(minLengthObj));
                Object maxLengthObj = object.get("maxLength");
                int maxLength = maxLengthObj instanceof Number
                        ? ((Number) maxLengthObj).intValue()
                        : Integer.parseInt(String.valueOf(maxLengthObj));

                return (T) field(name,
                        applyNullRate(faker, nullRate,
                                () -> {
                                    if (fieldLocale.equals(defaultLocale)) {
                                        return ((List) faker.collection((List) generators)
                                                .len(minLength, maxLength).build().get()).stream().toArray();
                                    } else {
                                        return faker.doWith(
                                                () -> ((List) faker.collection((List) generators)
                                                        .len(minLength, maxLength).build().get()).stream().toArray(),
                                                fieldLocale
                                        );
                                    }
                                }));
            case STRUCT:
                List<Field> fields = new ArrayList<>();
                List<Map<String, Object>> list = (List<Map<String, Object>>) object.get("fields");
                for (Map<String, Object> elem : list) {
                    fields.add(get(faker, elem, fieldLocale));
                }
                return (T) Field.compositeField(name, fields.toArray(new Field[0]));
            default:
                return (T) field(name, applyNullRate(faker, nullRate, generators.get(0)));
        }
    }

    private static Supplier<?> applyNullRate(Faker faker, double nullRate, Supplier<?> supplier) {
        if (nullRate <= 0) {
            return supplier;
        } else if (nullRate >= 1) {
            return () -> null;
        } else {
            return () -> faker.random().nextDouble() > nullRate ? supplier.get() : null;
        }
    }

    private static double parseNullRate(Object nullRateObj) {
        double nullRate;
        if (nullRateObj instanceof Number) {
            nullRate = ((Number) nullRateObj).doubleValue();
        } else {
            if (nullRateObj == null) {
                nullRate = 0d;
            } else {
                try {
                    nullRate = Double.parseDouble(nullRateObj.toString());
                } catch (NumberFormatException nfe) {
                    nullRate = 0d;
                }
            }
        }
        return nullRate;
    }

    private static Locale parseLocale(String fieldLocale, Locale defaultLocale) {
        if (fieldLocale == null) return defaultLocale;
        return Locale.forLanguageTag(fieldLocale);
    }

    private Method getMethod(Class clazz, String name) {
        Map<String, Method> name2method = CLASS2NAME2METHOD.get(clazz);
        if (name2method == null) {
            final Map<String, Method> map = new HashMap<>();
            for (Method m: clazz.getMethods()) {
                if (m.getParameterCount() == 0) {
                    map.put(m.getName().toLowerCase(Locale.ROOT), m);
                }
            }
            CLASS2NAME2METHOD.put(clazz, map);
            name2method = map;
        }
        final String loweredName = name.toLowerCase(Locale.ROOT);
        final Method method = name2method.get(loweredName);
        if (method == null) {
            throw new RuntimeException("Unknown call to " + clazz + "#" + name);
        }
        return method;
    }

    public List<Supplier<?>> getGenerators(Faker faker, List<String> list, Locale fieldLocale, Locale defaultLocale) {
        if (list == null) return null;
        List<Supplier<?>> res = new ArrayList<>(list.size());
        for (String s : list) {
            String[] c = s.split("#");
            try {
                if (c[0].indexOf('.') == -1 || c[0].contains("net.datafaker")) {
                    final Method m = getMethod(faker.getClass(), (
                            c[0].indexOf('.') == -1 ? c[0]
                                    : c[0].substring(c[0].lastIndexOf('.') + 1)));

                    if (fieldLocale.equals(defaultLocale)) {
                        final Object o = m.invoke(faker);
                        final Method method = o.getClass().getMethod(c[1]);
                        res.add(() -> {
                            try {
                                return method.invoke(o);
                            } catch (IllegalAccessException | InvocationTargetException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    } else {
                        res.add(() -> faker.doWith(() -> {
                            try {
                                final Object o = m.invoke(faker);
                                final Method method = o.getClass().getMethod(c[1]);
                                return method.invoke(o);
                            } catch (IllegalAccessException | InvocationTargetException e) {
                                throw new RuntimeException(e);
                            }
                        }, fieldLocale));
                    }
                } else {
                    if (fieldLocale.equals(defaultLocale)) {
                        Object o = Class.forName(c[0]).newInstance();
                        Method m = o.getClass().getMethod(c[1]);
                        res.add(() -> {
                            try {
                                return m.invoke(o);
                            } catch (IllegalAccessException | InvocationTargetException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    } else {
                        res.add(() -> faker.doWith(() -> {
                            try {
                                Object o = Class.forName(c[0]).newInstance();
                                Method m = o.getClass().getMethod(c[1]);
                                return m.invoke(o);
                            } catch (IllegalAccessException | InvocationTargetException e) {
                                throw new RuntimeException(e);
                            }
                        }, fieldLocale));
                    }
                }
            } catch (ClassNotFoundException | NoSuchMethodException e) {
                throw new RuntimeException("Issue for generator '" + s + "'", e);
            } catch (InvocationTargetException | IllegalAccessException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        }
        return res;
    }
}
