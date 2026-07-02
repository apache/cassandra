/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.simulator.asm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reflections.Reflections;
import org.reflections.scanners.FieldAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import org.apache.cassandra.utils.Nemesis;

import static java.util.Collections.emptyMap;
import static org.apache.cassandra.simulator.asm.InterceptClasses.dotsToSlashes;
import static org.apache.cassandra.simulator.asm.NemesisFieldKind.SIMPLE;

/**
 * Define classes that receive special handling.
 * At present all instance methods invoked on such classes have nemesis points inserted either side of them.
 *
 * Tests that need nemesis behavior on fields without annotating the source class can use
 * {@link #register(String, String, NemesisFieldKind)} to dynamically add entries.
 */
public class NemesisFieldSelectors
{
    public static final ConcurrentHashMap<String, Map<String, NemesisFieldKind>> classToFieldToNemesis;

    static
    {
        Map<Class<?>, NemesisFieldKind> byClass = new HashMap<>();
        for (NemesisFieldKind type : NemesisFieldKind.values())
            type.classes.forEach(c -> byClass.put(c, type));

        Stream.of(AtomicIntegerFieldUpdater.class, AtomicLongFieldUpdater.class, AtomicReferenceFieldUpdater.class)
              .forEach(c -> byClass.put(c, NemesisFieldKind.ATOMICUPDATERX));

        ConcurrentHashMap<String, Map<String, NemesisFieldKind>> byField = new ConcurrentHashMap<>();
        new Reflections(ConfigurationBuilder.build("org.apache.cassandra").addScanners(new FieldAnnotationsScanner()))
        .getFieldsAnnotatedWith(Nemesis.class)
        .forEach(field -> byField.computeIfAbsent(dotsToSlashes(field.getDeclaringClass()), ignore -> new ConcurrentHashMap<>())
                                 .put(field.getName(), byClass.getOrDefault(field.getType(), SIMPLE)));
        classToFieldToNemesis = byField;
    }

    /**
     * Register a field for nemesis handling without requiring a {@link Nemesis} annotation on the source class.
     * This allows tests to opt-in fields from classes they do not own.
     *
     * @param className the internal class name (slashes, e.g. "org/apache/cassandra/index/sai/disk/v1/vector/OnHeapGraph")
     * @param fieldName the field name as declared in the class
     * @param kind the nemesis field kind (typically {@link NemesisFieldKind#SIMPLE} for plain volatile fields,
     *             {@link NemesisFieldKind#ATOMICX} for AtomicInteger/AtomicLong/AtomicReference/AtomicBoolean fields)
     */
    public static void register(String className, String fieldName, NemesisFieldKind kind)
    {
        classToFieldToNemesis.computeIfAbsent(className, ignore -> new ConcurrentHashMap<>())
                             .put(fieldName, kind);
    }

    /**
     * Register a field for nemesis handling using the class object directly.
     *
     * @param clazz the class owning the field
     * @param fieldName the field name as declared in the class
     * @param kind the nemesis field kind
     */
    public static void register(Class<?> clazz, String fieldName, NemesisFieldKind kind)
    {
        register(dotsToSlashes(clazz), fieldName, kind);
    }

    /**
     * Remove a previously registered nemesis field. Useful for test cleanup.
     */
    public static void unregister(Class<?> clazz, String fieldName)
    {
        Map<String, NemesisFieldKind> fields = classToFieldToNemesis.get(dotsToSlashes(clazz));
        if (fields != null)
            fields.remove(fieldName);
    }

    public static NemesisFieldKind.Selector get()
    {
        return (name, field) -> classToFieldToNemesis.getOrDefault(name, emptyMap()).get(field);
    }
}
