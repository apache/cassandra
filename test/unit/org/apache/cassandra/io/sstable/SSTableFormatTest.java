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

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiFunction;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Config.SSTableConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

public class SSTableFormatTest
{
    public static abstract class AbstractFormat implements SSTableFormat<SSTableReader, SSTableWriter>
    {
        public Map<String, String> options;
        public String name;
        public String latestVersion;

        public AbstractFormat(String latestVersion)
        {
            this.latestVersion = latestVersion;
        }

        @Override
        public Version getVersion(String version)
        {
            Version v = Mockito.mock(Version.class);
            when(v.toString()).thenReturn(version);
            when(v.isCompatible()).thenReturn(version.charAt(0) == latestVersion.charAt(0));
            return v;
        }

        @Override
        public Version getLatestVersion()
        {
            return getVersion(latestVersion);
        }

        @Override
        public String name()
        {
            return name;
        }

        static class Factory implements SSTableFormat.Factory
        {
            public String name;
            public BiFunction<Map<String, String>, String, SSTableFormat<?, ?>> provider;

            public Factory(String name, BiFunction<Map<String, String>, String, SSTableFormat<?, ?>> provider)
            {
                this.name = name;
                this.provider = provider;
            }

            @Override
            public String name()
            {
                return name;
            }

            @Override
            public SSTableFormat<?, ?> getInstance(Map<String, String> options)
            {
                return provider.apply(options, name);
            }
        }
    }

    public static AbstractFormat.Factory factory(String name, Class<? extends AbstractFormat> clazz)
    {
        return new AbstractFormat.Factory(name, (options, version) -> {
            AbstractFormat format = Mockito.spy(clazz);
            format.name = name;
            format.options = options;
            return format;
        });
    }

    public static abstract class Format1 extends AbstractFormat
    {
        public Format1()
        {
            super("xx");
        }
    }

    public static abstract class Format2 extends AbstractFormat
    {
        public Format2()
        {
            super("yy");
        }
    }

    public static abstract class Format3 extends AbstractFormat
    {
        public Format3()
        {
            super("zz");
        }
    }

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.clientInitialization();
    }

    private static final String yamlContent0 = "";
    private static final SSTableConfig expected0 = new Config.SSTableConfig();

    private static final String yamlContent1 = "sstable:\n" +
                                               "   selected_format: aaa\n";
    private static final SSTableConfig expected1 = new Config.SSTableConfig()
    {
        {
            selected_format = "aaa";
        }
    };

    private static final String yamlContent2 = "sstable:\n" +
                                               "   selected_format: aaa\n" +
                                               "   format:\n" +
                                               "       aaa:\n" +
                                               "           param1: value1\n" +
                                               "           param2: value2\n" +
                                               "       bbb:\n" +
                                               "           param3: value3\n" +
                                               "           param4: value4\n";

    private static final Config.SSTableConfig expected2 = new SSTableConfig()
    {
        {
            selected_format = "aaa";
            format = ImmutableMap.of("aaa", ImmutableMap.of("param1", "value1", "param2", "value2"),
                                     "bbb", ImmutableMap.of("param3", "value3", "param4", "value4"));
        }
    };

    private static final SSTableConfig unexpected = new Config.SSTableConfig()
    {
        {
            selected_format = "aaa";
        }
    };


    @Test
    public void testParsingYamlConfig() throws IOException
    {
        YamlConfigurationLoader loader = new YamlConfigurationLoader();
        File f = FileUtils.createTempFile("sstable_format_test_config", ".yaml");
        URL url = f.toPath().toUri().toURL();

        ImmutableMap.of(yamlContent0, expected0, yamlContent1, expected1, yamlContent2, expected2).forEach((yamlContent, expected) -> {
            try (FileOutputStreamPlus out = f.newOutputStream(File.WriteMode.OVERWRITE))
            {
                out.write(yamlContent.getBytes());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            Config config = loader.loadConfig(url);
            assertThat(config.sstable).describedAs("Yaml: \n%s\n", yamlContent).isEqualToComparingFieldByField(expected);
        });
    }

    @Before
    public void before()
    {
    }

    public static void configure(SSTableConfig config, SSTableFormat.Factory... factories)
    {
        DatabaseDescriptor.resetSSTableFormats(Arrays.asList(factories), config);
    }

    private void verifyFormat(String name, Map<String, String> options)
    {
        AbstractFormat format = (AbstractFormat) DatabaseDescriptor.getSSTableFormats().get(name);
        assertThat(format.name).isEqualTo(name);
        assertThat(format.options).isEqualTo(options);
    }

    private void verifySelectedFormat(String name)
    {
        assertThat(DatabaseDescriptor.getSelectedSSTableFormat().name()).isEqualTo(name);
    }

    @Test
    public void testValidConfig()
    {
        configure(expected1, factory("aaa", Format1.class));
        assertThat(DatabaseDescriptor.getSSTableFormats()).hasSize(1);
        verifyFormat("aaa", ImmutableMap.of());
        verifySelectedFormat("aaa");

        configure(expected2, factory("aaa", Format1.class), factory("bbb", Format2.class), factory("ccc", Format3.class));
        assertThat(DatabaseDescriptor.getSSTableFormats()).hasSize(3);
        verifyFormat("aaa", ImmutableMap.of("param1", "value1", "param2", "value2"));
        verifyFormat("bbb", ImmutableMap.of("param3", "value3", "param4", "value4"));
        verifyFormat("ccc", ImmutableMap.of());
        verifySelectedFormat("aaa");
    }

    @Test
    public void testConfigValidation()
    {
        // invalid name
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> configure(expected1, factory("Aa", Format1.class)))
                                                               .withMessageContainingAll("SSTable format name", "must be non-empty, lower-case letters only string");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> configure(expected1, factory("a-a", Format1.class)))
                                                               .withMessageContainingAll("SSTable format name", "must be non-empty, lower-case letters only string");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> configure(expected1, factory("a1", Format1.class)))
                                                               .withMessageContainingAll("SSTable format name", "must be non-empty, lower-case letters only string");
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> configure(expected1, factory("", Format1.class)))
                                                               .withMessageContainingAll("SSTable format name", "must be non-empty, lower-case letters only string");

        // duplicate name
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> configure(expected1, factory("aaa", Format1.class), factory("aaa", Format2.class)))
                                                               .withMessageContainingAll("Multiple sstable format implementations with the same name", "aaa");

        // missing name
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> configure(expected1, factory(null, Format1.class)))
                                                               .withMessageContainingAll("SSTable format name", "cannot be null");

        // Configuration contains options of unknown sstable formats
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> configure(expected2, factory("aaa", Format1.class)))
                                                               .withMessageContainingAll("Configuration contains options of unknown sstable formats", "bbb");

        // Selected sstable format '%s' is not available
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> configure(expected1, factory("bbb", Format1.class)))
                                                               .withMessageContainingAll("Selected sstable format", "aaa", "is not available");
    }
}
