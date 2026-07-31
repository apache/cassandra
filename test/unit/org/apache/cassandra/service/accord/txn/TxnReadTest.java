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

package org.apache.cassandra.service.accord.txn;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.accord.txn.TxnRead.ImportMetadata;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.TimeUUID;

import static accord.utils.Property.qt;
import static org.apache.cassandra.index.accord.RouteIndexTest.rangeGen;
import static org.apache.cassandra.utils.CassandraGenerators.TABLE_METADATA_GEN;
import static org.apache.cassandra.utils.Generators.toGen;

public class TxnReadTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void importMetaDataSerde()
    {
        DataOutputBuffer output = new DataOutputBuffer();
        qt().check(rs -> {
            ImportMetadata importMetadata = new ImportMetadata(toGen(Generators.UUID_RANDOM_GEN).next(rs), toGen(Generators.timeUUID()).next(rs), rs.nextLong());
            Serializers.testSerde(output, ImportMetadata.serializer, importMetadata, Version.LATEST);
        });
    }

    @Test
    public void importReadSerde()
    {
        DataOutputBuffer output = new DataOutputBuffer();
        qt().check(rs -> {
            int length = rs.nextInt(1, 10);
            List<TableMetadata> tables = new ArrayList<>(length);
            for (int i = 0; i < length; i++)
                tables.add(toGen(TABLE_METADATA_GEN).next(rs));
            TableMetadatas tableMetadatas = TableMetadatas.of(tables);
            UUID uuid = toGen(Generators.UUID_RANDOM_GEN).next(rs);
            TimeUUID timeUUID = toGen(Generators.timeUUID()).next(rs);
            long epoch = rs.nextLong();
            TokenRange range = rangeGen(rs, tables.stream().map(TableMetadata::id).collect(Collectors.toList())).next(rs);
            TxnRead txnRead = TxnRead.createImport(tableMetadatas, range, uuid, timeUUID, epoch);
            Serializers.testSerde(output, TxnRead.serializer, txnRead, tablesAndKeys(txnRead), Version.LATEST);
        });
    }

    private static TableMetadatasAndKeys tablesAndKeys(TxnRead read)
    {
        return new TableMetadatasAndKeys(read.tables, read.keys());
    }
}
