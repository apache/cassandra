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
package org.apache.cassandra.config;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.yaml.snakeyaml.introspector.Property;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ValidatedPropertyLoaderTest
{
    private static final Loader LOADER = Properties.validatedPropertyLoader();

    @Test
    public void testListenablePropertyMethodPattern() throws Exception
    {
        assertThatThrownBy(() -> LOADER.getProperties(TestConfig.class)
                                       .get("test_property")
                                       .set(new TestConfig(), 42))
        .hasRootCauseInstanceOf(ConfigurationException.class)
        .hasRootCauseMessage("Value must not be equal to 42");
    }

    @Test
    public void testListenablePropertyExists() throws Exception
    {
        assertThat(LOADER.getProperties(TestConfig.class).get("test_property")).isNotNull();
        assertThat(LOADER.getProperties(TestConfig.class).get("test_property")).isInstanceOf(ListenableProperty.class);
    }

    @Test
    public void testListenablePropertyNotExists() throws Exception
    {
        assertThat(LOADER.getProperties(TestConfig.class).get("test_property_not_exists")).isNull();
    }

    @Test
    public void testListenablePropertySet() throws Exception
    {
        TestConfig conf = new TestConfig();
        LOADER.getProperties(TestConfig.class).get("test_property").set(conf, 40);
        assertThat(conf.test_property).isEqualTo(40);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testListenablePropertyCustomListener() throws Exception
    {
        Property property = LOADER.getProperties(TestConfig.class).get("test_property");
        assertThat(LOADER.getProperties(TestConfig.class).get("test_property")).isInstanceOf(ListenableProperty.class);

        ListenableProperty<TestConfig, Integer> listenableProperty = (ListenableProperty<TestConfig, Integer>) property;
        listenableProperty.addBeforeListener(ListenableProperty.BeforeChangeListener.consume((conf, value) -> {
            if (value > 100)
                throw new ConfigurationException("Value must not be greater than 100");
        }));

        TestConfig conf = new TestConfig();
        assertThatThrownBy(() -> property.set(conf, 200))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("Value must not be greater than 100");

        LOADER.getProperties(TestConfig.class).get("test_property").set(conf, 10);
        assertThat(conf.test_property).isEqualTo(10);
    }

    public static class TestConfig
    {
        @ValidatedBy(useClass = "org.apache.cassandra.config.ValidatedPropertyLoaderTest$TestConfig", useClassMethod = "validateTestPropertyInvalid")
        public Integer test_property = 0;

        public static void validateTestPropertyInvalid(TestConfig conf, Integer value)
        {
            if (value == 42)
                throw new ConfigurationException("Value must not be equal to 42");
        }
    }
}