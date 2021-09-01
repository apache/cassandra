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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.apache.cassandra.utils.Nemesis;
import org.reflections.Reflections;
import org.reflections.scanners.FieldAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import static java.util.Collections.emptyMap;
import static org.apache.cassandra.simulator.asm.InterceptClasses.dotsToSlashes;
import static org.apache.cassandra.simulator.asm.NemesisFieldKind.SIMPLE;

/**
 * Define classes that receive special handling.
 * At present all instance methods invoked on such classes have nemesis points inserted either side of them.
 */
public class NemesisFieldSelectors
{
    public static final Map<String, Map<String, NemesisFieldKind>> classToFieldToNemesis;

    static
    {
        Map<Class<?>, NemesisFieldKind> byClass = new HashMap<>();
        for (NemesisFieldKind type : NemesisFieldKind.values())
            type.classes.forEach(c -> byClass.put(c, type));

        Stream.of(AtomicIntegerFieldUpdater.class, AtomicLongFieldUpdater.class, AtomicReferenceFieldUpdater.class)
              .forEach(c -> byClass.put(c, NemesisFieldKind.ATOMICUPDATERX));

        Map<String, Map<String, NemesisFieldKind>> byField = new HashMap<>();
        new Reflections(ConfigurationBuilder.build("org.apache.cassandra").addScanners(new FieldAnnotationsScanner()))
        .getFieldsAnnotatedWith(Nemesis.class)
        .forEach(field -> byField.computeIfAbsent(dotsToSlashes(field.getDeclaringClass()), ignore -> new HashMap<>())
                                 .put(field.getName(), byClass.getOrDefault(field.getType(), SIMPLE)));
        classToFieldToNemesis = Collections.unmodifiableMap(byField);
    }

    public static NemesisFieldKind.Selector get()
    {
        return (name, field) -> classToFieldToNemesis.getOrDefault(name, emptyMap()).get(field);
    }
}
