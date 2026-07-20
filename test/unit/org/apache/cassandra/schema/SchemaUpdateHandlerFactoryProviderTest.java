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

package org.apache.cassandra.schema;

import org.junit.Test;

import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ClassLoadingTestNonAssignable;
import org.apache.cassandra.utils.ClassLoadingTestSupport;

import static org.apache.cassandra.schema.SchemaUpdateHandlerFactoryProvider.SUH_FACTORY_CLASS_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SchemaUpdateHandlerFactoryProviderTest
{
    @Test
    public void testDefaultFactoryWhenPropertyUnset()
    {
        assertThat(SchemaUpdateHandlerFactoryProvider.instance.get()).isSameAs(DefaultSchemaUpdateHandlerFactory.instance);
    }

    @Test
    public void testConfiguredFactoryResolves()
    {
        try (WithProperties properties = new WithProperties(SUH_FACTORY_CLASS_PROPERTY, DefaultSchemaUpdateHandlerFactory.class.getName()))
        {
            assertThat(SchemaUpdateHandlerFactoryProvider.instance.get()).isInstanceOf(DefaultSchemaUpdateHandlerFactory.class);
        }
    }

    @Test
    public void testRejectsNonFactoryWithoutInitializing()
    {
        ClassLoadingTestSupport.assertNotInitialized(ClassLoadingTestNonAssignable.class);
        try (WithProperties properties = new WithProperties(SUH_FACTORY_CLASS_PROPERTY, ClassLoadingTestNonAssignable.class.getName()))
        {
            assertThatThrownBy(() -> SchemaUpdateHandlerFactoryProvider.instance.get())
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("must extend or implement " + SchemaUpdateHandlerFactory.class.getName());
        }

        assertThat(ClassLoadingTestSupport.wasInitialized(ClassLoadingTestNonAssignable.class)).isFalse();
    }

    @Test
    public void testUnknownFactoryClassRejected()
    {
        try (WithProperties properties = new WithProperties(SUH_FACTORY_CLASS_PROPERTY, "does.not.ExistFactory"))
        {
            assertThatThrownBy(() -> SchemaUpdateHandlerFactoryProvider.instance.get())
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("Unable to find schema update handler factory class");
        }
    }
}
