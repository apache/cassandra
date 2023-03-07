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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.AbstractSSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.utils.Pair;
import org.mockito.Mockito;

import static org.apache.cassandra.config.Config.SSTABLE_FORMAT_ID;
import static org.apache.cassandra.config.Config.SSTABLE_FORMAT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class SSTableFormatTypeTest
{
    public static abstract class Format1 extends AbstractSSTableFormat<SSTableReader, SSTableWriter>
    {
        public static Format1 instance = null;
    }

    public static abstract class Format2 extends AbstractSSTableFormat<SSTableReader, SSTableWriter>
    {
        public static Format2 instance = null;
    }

    public static abstract class NonSSTableFormat
    {
        public static NonSSTableFormat instance = null;
    }

    public abstract static class BrokenSSTableFormat extends AbstractSSTableFormat<SSTableReader, SSTableWriter>
    {
        public BrokenSSTableFormat()
        {
            // initialization will fail because it is abstract class
        }
    }

    private Map<String, Supplier<SSTableFormat<?, ?>>> factories;
    Pair<List<SSTableFormat.Type>, SSTableFormat.Type[]> formats;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Before
    public void before()
    {
        Format1.instance = Mockito.spy(Format1.class);
        Format2.instance = Mockito.spy(Format2.class);
        NonSSTableFormat.instance = Mockito.spy(NonSSTableFormat.class);
    }

    @Test
    public void testValidConfig()
    {
        init(format(Format1.class, "0", "aaa"), format(Format2.class, "1", "bbb"));
        verifyFormat(Format1.class, 0, "aaa");
        verifyFormat(Format2.class, 1, "bbb");
        assertThat(formats.left.get(0).name).isEqualTo("aaa");
    }

    @Test
    public void testDefaultSelectedByOrder()
    {
        init(format(Format1.class, "1", "aaa"), format(Format2.class, "0", "bbb"));
        verifyFormat(Format1.class, 1, "aaa");
        verifyFormat(Format2.class, 0, "bbb");
        assertThat(formats.left.get(0).name).isEqualTo("aaa");
    }

    @Test
    public void testNonConsecutiveOrdinals()
    {
        init(format(Format1.class, "5", "aaa"), format(Format2.class, "10", "bbb"));
        verifyFormat(Format1.class, 5, "aaa");
        verifyFormat(Format2.class, 10, "bbb");
        assertThat(formats.left.get(0).name).isEqualTo("aaa");
    }

    @Test
    public void testConfigValidation()
    {
        // invalid name
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> load(format(Format1.class, "0", "Aa")))
                                                               .withMessageContainingAll("'name'", "Format1", "must be non-empty, lower-case letters only string");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> load(format(Format1.class, "0", "a-a")))
                                                               .withMessageContainingAll("'name'", "Format1", "must be non-empty, lower-case letters only string");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> load(format(Format1.class, "0", "a1")))
                                                               .withMessageContainingAll("'name'", "Format1", "must be non-empty, lower-case letters only string");

        // invalid id
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> load(format(Format1.class, "-1", "aaa")))
                                                               .withMessageContainingAll("'id'", "Format1", "must be within bounds [0..127] range");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> load(format(Format1.class, "128", "aaa")))
                                                               .withMessageContainingAll("'id'", "Format1", "must be within bounds [0..127] range");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> load(format(Format1.class, "x", "aaa")))
                                                               .withMessageContainingAll("'id'", "Format1", "must be an integer");

        // duplicate name
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> load(format(Format1.class, "0", "aaa"), format(Format2.class, "1", "aaa")))
                                                               .withMessageContainingAll("Name 'aaa' of sstable format", "Format2", "is already defined");

        // duplicate id
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> load(format(Format1.class, "0", "aaa"), format(Format2.class, "0", "bbb")))
                                                               .withMessageContainingAll("ID '0' of sstable format", "Format2", "is already defined");

        // missing name
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> load(format(Format1.class.getName(), ImmutableMap.of(SSTABLE_FORMAT_ID, "0"))))
                                                               .withMessageContainingAll("Missing 'name' parameter", "Format1");

        // missing id
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> load(format(Format1.class.getName(), ImmutableMap.of(SSTABLE_FORMAT_NAME, "aaa"))))
                                                               .withMessageContainingAll("Missing 'id' parameter", "Format1");

        // not an sstable format class
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> init(format(NonSSTableFormat.class, "0", "aaa")))
                                                               .withMessageContainingAll("NonSSTableFormat", "format aaa", "does not implement org.apache.cassandra.io.sstable.format.SSTableFormat");

        // missing class
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> init(format("org.apache.cassandra.io.sstable.format.dummy.DummyFormat", ImmutableMap.of(SSTABLE_FORMAT_ID, "0", SSTABLE_FORMAT_NAME, "aaa"))))
                                                               .withMessageContainingAll("Unable to find sstable format class", "org.apache.cassandra.io.sstable.format.dummy.DummyFormat");

        // failed initialization
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> init(format(BrokenSSTableFormat.class, "0", "aaa")))
                                                               .withMessageContainingAll(BrokenSSTableFormat.class.getName(), "sstable format");
    }

    private void verifyFormat(Class<?> expectedClass, int expectedOrdinal, String expectedName)
    {
        SSTableFormat.Type formatType = formats.right[expectedOrdinal];
        assertThat(formatType).isNotNull();
        assertThat(formatType.name).isEqualTo(expectedName);
        assertThat(formatType.ordinal).isEqualTo(expectedOrdinal);
        assertThat(formatType.info).isInstanceOf(expectedClass);
        assertThat(formats.left.stream().filter(f -> f.name.equals(expectedName)).findFirst().get()).isSameAs(formatType);
    }

    private ParameterizedClass format(Class<?> clazz, String id, String name, String... otherParams)
    {
        ImmutableMap.Builder<String, String> b = ImmutableMap.builder();
        b.put(SSTABLE_FORMAT_ID, id);
        b.put(SSTABLE_FORMAT_NAME, name);
        for (int i = 0; i < otherParams.length; i += 2)
            b.put(otherParams[i], otherParams[i + 1]);
        return format(clazz.getName(), b.build());
    }

    private ParameterizedClass format(String className, Map<String, String> params)
    {
        return new ParameterizedClass(className, params);
    }

    private Map<String, Supplier<SSTableFormat<?, ?>>> load(ParameterizedClass... formats)
    {
        return DatabaseDescriptor.loadSSTableFormatFactories(Arrays.asList(formats));
    }

    private void init(ParameterizedClass... formats)
    {
        this.factories = load(formats);
        this.formats = SSTableFormat.Type.readFactories(factories);
    }
}
