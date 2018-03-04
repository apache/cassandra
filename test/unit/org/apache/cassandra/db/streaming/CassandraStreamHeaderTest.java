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

package org.apache.cassandra.db.streaming;

import java.util.ArrayList;

import org.junit.Test;

import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.SerializationUtils;

public class CassandraStreamHeaderTest
{
    @Test
    public void serializerTest()
    {
        String ddl = "CREATE TABLE tbl (k INT PRIMARY KEY, v INT)";
        TableMetadata metadata = CreateTableStatement.parse(ddl, "ks").build();
        CassandraStreamHeader header = new CassandraStreamHeader(BigFormat.latestVersion,
                                                                 SSTableFormat.Type.BIG,
                                                                 0,
                                                                 new ArrayList<>(),
                                                                 ((CompressionMetadata) null),
                                                                 0,
                                                                 SerializationHeader.makeWithoutStats(metadata).toComponent());

        SerializationUtils.assertSerializationCycle(header, CassandraStreamHeader.serializer);
    }
}
