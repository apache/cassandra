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

package org.apache.cassandra.service;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;

import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.apache.cassandra.service.CassandraDaemon.SPECULATION_THRESHOLD_UPDATER;

public class OptionalTasksTest
{
    private static final String KEYSPACE = "OpitonalTasksTest";
    private static final String TABLE = "DroppedTable";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), standardCFMD(KEYSPACE, TABLE));
    }
    
    @Test
    public void shouldIgnoreDroppedTable()
    {
        // Remove the literal Keyspace and TableMetadataRef objects from Schema...
        Schema.instance.removeKeyspaceInstance(KEYSPACE);
        Schema.instance.removeTableMetadataRef(KEYSPACE, TABLE);
        
        // ...and ensure that the speculation threshold updater does not allow an exception to escape.
        SPECULATION_THRESHOLD_UPDATER.run();
    }
}
