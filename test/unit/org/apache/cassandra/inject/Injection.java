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
package org.apache.cassandra.inject;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Refer to <a href="https://github.com/bytemanproject/byteman/blob/master/docs/asciidoc/src/main/asciidoc/chapters/Byteman-Rule-Language.adoc"/>
 * and injections.md files in this directory.
 */
public class Injection
{
    private static final Map<String, AtomicBoolean> enableFlags = new ConcurrentHashMap<>();
    private final String id;
    private final Rule[] rules;

    public Injection(Rule[] rules)
    {
        this(UUID.randomUUID().toString(), rules);
    }

    public Injection(String id, Rule[] rules)
    {
        this.id = id;
        this.rules = rules;
        enable();
    }

    public String format()
    {
        return Arrays.stream(rules).map(rule -> rule.script).collect(Collectors.joining("\n"));
    }

    public String[] getClassesToPreload()
    {
        return Arrays.stream(rules).filter(r -> r.classToPreload != null).map(r -> r.classToPreload).toArray(String[]::new);
    }

    public void enable()
    {
        enableFlags.computeIfAbsent(id, id -> new AtomicBoolean()).set(true);
    }

    public void disable()
    {
        enableFlags.computeIfAbsent(id, id -> new AtomicBoolean()).set(false);
    }

    public boolean isEnabled()
    {
        return enableFlags.computeIfAbsent(id, id -> new AtomicBoolean(true)).get();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface CheckEnabled {}

    @CheckEnabled
    public static boolean checkEnabled(String id)
    {
        return enableFlags.computeIfAbsent(id, i -> new AtomicBoolean(true)).get();
    }
}
