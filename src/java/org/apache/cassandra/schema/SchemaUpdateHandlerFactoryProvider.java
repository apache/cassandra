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

import javax.inject.Provider;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.SCHEMA_UPDATE_HANDLER_FACTORY_CLASS;

/**
 * Provides the instance of SchemaUpdateHandler factory pointed by
 * {@link org.apache.cassandra.config.CassandraRelevantProperties#SCHEMA_UPDATE_HANDLER_FACTORY_CLASS} system property.
 * If the property is not defined, the default factory {@link DefaultSchemaUpdateHandler} instance is returned.
 */
public class SchemaUpdateHandlerFactoryProvider implements Provider<SchemaUpdateHandlerFactory>
{
    /** @deprecated Use CassandraRelevantProperties.SCHEMA_UPDATE_HANDLER_FACTORY_CLASS instead. See CASSANDRA-17797 */
    @Deprecated(since = "5.0")
    public static final String SUH_FACTORY_CLASS_PROPERTY = "cassandra.schema.update_handler_factory.class";

    public final static SchemaUpdateHandlerFactoryProvider instance = new SchemaUpdateHandlerFactoryProvider();

    @Override
    public SchemaUpdateHandlerFactory get()
    {
        String suhFactoryClassName = StringUtils.trimToNull(SCHEMA_UPDATE_HANDLER_FACTORY_CLASS.getString());
        if (suhFactoryClassName == null)
        {
            return DefaultSchemaUpdateHandlerFactory.instance;
        }
        else
        {
            Class<SchemaUpdateHandlerFactory> suhFactoryClass = FBUtilities.classForName(suhFactoryClassName, "schema update handler factory");
            try
            {
                return suhFactoryClass.newInstance();
            }
            catch (InstantiationException | IllegalAccessException ex)
            {
                throw new ConfigurationException(String.format("Failed to initialize schema update handler factory class %s defined in %s system property.",
                                                               suhFactoryClassName, SCHEMA_UPDATE_HANDLER_FACTORY_CLASS.getKey()), ex);
            }
        }
    }
}
