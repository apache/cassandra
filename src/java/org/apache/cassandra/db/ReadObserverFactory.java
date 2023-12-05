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
package org.apache.cassandra.db;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_READ_OBSERVER_FACTORY;


/**
 * Provides custom factory that creates a {@link ReadObserver} instance per read request
 */
public interface ReadObserverFactory
{
    ReadObserverFactory instance = CUSTOM_READ_OBSERVER_FACTORY.getString() == null ?
                                   new ReadObserverFactory() {} :
                                   FBUtilities.construct(CassandraRelevantProperties.CUSTOM_READ_OBSERVER_FACTORY.getString(), "custom read observer factory");

    default ReadObserver create(TableMetadata table)
    {
        return ReadObserver.NO_OP;
    }
}
