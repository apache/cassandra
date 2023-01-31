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

package org.apache.cassandra.distributed.test;

import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;

public class Cassandra18153ReproducerTest extends TestBaseImpl
{
    @Test
    public void test() throws Exception
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2, 1))
                                        .start())
        {
            // flush keyspace
            cluster.get(1).flush("system");


            String result = cluster.get(1).appliesOnInstance((String ks, String tbl) -> {
                ColumnFamilyStore columnFamilyStore = Keyspace.open(ks).getColumnFamilyStore(tbl);
                Set<SSTableReader> liveSSTables = columnFamilyStore.getLiveSSTables();

                StringBuilder sb = new StringBuilder();

                for (SSTableReader reader : liveSSTables)
                {
                    sb.append(reader.descriptor)
                      .append(" -> ")
                      .append(reader.getSSTableMetadata().originatingHostId)
                      .append(System.lineSeparator());
                }

                sb.append("local host id -> ")
                  .append(StorageService.instance.getLocalHostUUID())
                  .append(System.lineSeparator());

                return sb.toString();
            }).apply("system", "local");

            System.out.println(result);
        }
    }
}
