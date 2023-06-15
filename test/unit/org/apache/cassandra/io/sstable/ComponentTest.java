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

package org.apache.cassandra.io.sstable;

import java.util.HashSet;
import java.util.function.Function;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Component.Type;
import org.apache.cassandra.io.sstable.SSTableFormatTest.Format1;
import org.apache.cassandra.io.sstable.SSTableFormatTest.Format2;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.mockito.Mockito;

import static org.apache.cassandra.io.sstable.SSTableFormatTest.factory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ComponentTest
{
    private static final String SECOND = "second";
    private static final String FIRST = "first";

    static
    {
        DatabaseDescriptor.daemonInitialization(() -> {
            Config config = DatabaseDescriptor.loadConfig();
            SSTableFormatTest.configure(new Config.SSTableConfig(), new BigFormat.BigFormatFactory(), factory("first", Format1.class), factory("second", Format2.class));
            return config;
        });
    }

    @Test
    public void testTypes()
    {
        Function<Type, Component> componentFactory = Mockito.mock(Function.class);

        // do not allow to define a type with the same name or repr as the existing type for this or parent format
        assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> Type.createSingleton(Components.Types.TOC.name, Components.Types.TOC.repr + "x", true, Format1.class));
        assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> Type.createSingleton(Components.Types.TOC.name + "x", Components.Types.TOC.repr, true, Format2.class));

        // allow to define a format with other name and repr
        Type t1 = Type.createSingleton("ONE", "One.db", true, Format1.class);

        // allow to define a format with the same name and repr for two different formats
        Type t2f1 = Type.createSingleton("TWO", "Two.db", true, Format1.class);
        Type t2f2 = Type.createSingleton("TWO", "Two.db", true, Format2.class);
        assertThat(t2f1).isNotEqualTo(t2f2);

        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> Type.createSingleton(null, "-Three.db", true, Format1.class));

        assertThat(Type.fromRepresentation("should be custom", BigFormat.getInstance())).isSameAs(Components.Types.CUSTOM);
        assertThat(Type.fromRepresentation(Components.Types.TOC.repr, BigFormat.getInstance())).isSameAs(Components.Types.TOC);
        assertThat(Type.fromRepresentation(t1.repr, DatabaseDescriptor.getSSTableFormats().get(FIRST))).isSameAs(t1);
        assertThat(Type.fromRepresentation(t2f1.repr, DatabaseDescriptor.getSSTableFormats().get(FIRST))).isSameAs(t2f1);
        assertThat(Type.fromRepresentation(t2f2.repr, DatabaseDescriptor.getSSTableFormats().get(SECOND))).isSameAs(t2f2);
    }

    @Test
    public void testComponents()
    {
        Type t3f1 = Type.createSingleton("THREE", "Three.db", true, Format1.class);
        Type t3f2 = Type.createSingleton("THREE", "Three.db", true, Format2.class);
        Type t4f1 = Type.create("FOUR", ".*-Four.db", true, Format1.class);
        Type t4f2 = Type.create("FOUR", ".*-Four.db", true, Format2.class);

        Component c1 = t3f1.getSingleton();
        Component c2 = t3f2.getSingleton();

        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> t3f1.createComponent(t3f1.repr));
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> t3f2.createComponent(t3f2.repr));
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> t4f1.getSingleton());
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> t4f2.getSingleton());

        assertThat(Component.parse(t3f1.repr, DatabaseDescriptor.getSSTableFormats().get(FIRST))).isSameAs(c1);
        assertThat(Component.parse(t3f2.repr, DatabaseDescriptor.getSSTableFormats().get("second"))).isSameAs(c2);
        assertThat(c1).isNotEqualTo(c2);
        assertThat(c1.type).isSameAs(t3f1);
        assertThat(c2.type).isSameAs(t3f2);

        Component c3 = Component.parse("abc-Four.db", DatabaseDescriptor.getSSTableFormats().get(FIRST));
        Component c4 = Component.parse("abc-Four.db", DatabaseDescriptor.getSSTableFormats().get("second"));
        assertThat(c3.type).isSameAs(t4f1);
        assertThat(c4.type).isSameAs(t4f2);
        assertThat(c3.name).isEqualTo("abc-Four.db");
        assertThat(c4.name).isEqualTo("abc-Four.db");
        assertThat(c3).isNotEqualTo(c4);
        assertThat(c3).isNotEqualTo(c1);
        assertThat(c4).isNotEqualTo(c2);

        Component c5 = Component.parse("abc-Five.db", DatabaseDescriptor.getSSTableFormats().get(FIRST));
        assertThat(c5.type).isSameAs(Components.Types.CUSTOM);
        assertThat(c5.name).isEqualTo("abc-Five.db");

        Component c6 = Component.parse("Data.db", DatabaseDescriptor.getSSTableFormats().get("second"));
        assertThat(c6.type).isSameAs(Components.Types.DATA);
        assertThat(c6).isSameAs(Components.DATA);

        HashSet<Component> s1 = Sets.newHashSet(Component.getSingletonsFor(Format1.class));
        HashSet<Component> s2 = Sets.newHashSet(Component.getSingletonsFor(Format2.class));
        assertThat(s1).contains(c1, Components.DATA, Components.STATS, Components.COMPRESSION_INFO);
        assertThat(s2).contains(c2, Components.DATA, Components.STATS, Components.COMPRESSION_INFO);
        assertThat(s1).doesNotContain(c2);
        assertThat(s2).doesNotContain(c1);

        assertThat(Sets.newHashSet(Component.getSingletonsFor(DatabaseDescriptor.getSSTableFormats().get(FIRST)))).isEqualTo(s1);
        assertThat(Sets.newHashSet(Component.getSingletonsFor(DatabaseDescriptor.getSSTableFormats().get("second")))).isEqualTo(s2);
    }
}
