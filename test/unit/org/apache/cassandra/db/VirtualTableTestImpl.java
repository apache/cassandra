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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

public class VirtualTableTestImpl extends InMemoryVirtualTable
{
    static
    {
        Map<String, CQL3Type> definitions = new HashMap<>();
        definitions.put("p1", CQL3Type.Native.INT);
        definitions.put("p2", CQL3Type.Native.INT);
        definitions.put("c1", CQL3Type.Native.INT);
        definitions.put("c2", CQL3Type.Native.INT);
        definitions.put("v1", CQL3Type.Native.INT);
        definitions.put("v2", CQL3Type.Native.INT);

        schemaBuilder(definitions)
                .addKey("p1")
                .addKey("p2")
                .addClustering("c1")
                .addClustering("c2")
                .register();
    }

    public List<Pair<DecoratedKey, Row>> inserts = new ArrayList();
    public VirtualTableTestImpl(TableMetadata metadata)
    {
        super(metadata);
    }

    public Comparator<DecoratedKey> comp = (p1, p2) ->
    {
        return metadata.partitionKeyType.compare(p1.getKey(), p2.getKey());
    };

    public Comparator<DecoratedKey> partitionComparator()
    {
        return comp;
    }

    public boolean writable()
    {
        return true;
    }

    public boolean allowFiltering()
    {
        return true;
    }

    /**
     * Execute an update operation.
     *
     * @param partitionKey partition key for the update.
     * @param params parameters of the update.
     */
    public void mutate(DecoratedKey partitionKey, Row row) throws CassandraException
    {
        inserts.add(Pair.create(partitionKey, row));
    }

    public void read(StatementRestrictions restrictions, QueryOptions options, ResultBuilder result)
    {
        for (int p1 = 0; p1 < 3; p1++)
        {
            for (int p2 = 0; p2 < 7; p2++)
            {
                for (int c1 = 0; c1 < 7; c1++)
                {
                    for (int c2 = 0; c2 < 3; c2++)
                    {
                        result.row(p1, p2, c1, c2)
                                .column("v1", p1 + p2 * c1 + c2)
                                .column("v2", p1 * p2 + c1 * c2)
                                .endRow();
                    }
                }
            }
        }
    }

}
