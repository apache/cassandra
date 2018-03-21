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
package org.apache.cassandra.db.virtual;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.VirtualTable;
import org.apache.cassandra.db.VirtualTable.VirtualSchema;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.schema.TableMetadata;

/**
 * For use with cassandra-stress for checking more of the network/client side of things. run normally:
 * $ cassandra-stress write
 * 
 * then remove the table it created and replace with this
 * $ cqlsh
 * cqlsh> DROP TABLE keyspace1.standard1;
 * cqlsh> CREATE TABLE USING StressNoop keyspace1.standard1;
 * 
 * rerun
 * $ cassandra-stress write
 */
public class StressNoop extends VirtualTable
{
    private static final String C4 = "C4";
    private static final String C3 = "C3";
    private static final String C2 = "C2";
    private static final String C1 = "C1";
    private static final String C0 = "C0";
    private static final String KEY = "key";

    static
    {
        Map<String, CQL3Type> definitions = new HashMap<>();
        definitions.put(KEY, CQL3Type.Native.BLOB);
        definitions.put(C0, CQL3Type.Native.BLOB);
        definitions.put(C1, CQL3Type.Native.BLOB);
        definitions.put(C2, CQL3Type.Native.BLOB);
        definitions.put(C3, CQL3Type.Native.BLOB);
        definitions.put(C4, CQL3Type.Native.BLOB);

        schemaBuilder(definitions)
                .addKey(KEY)
                .register();
    }

    public StressNoop(TableMetadata metadata)
    {
        super(metadata);
    }

    public boolean writable()
    {
        return true;
    }

    public void mutate(DecoratedKey partitionKey, Row row) throws CassandraException
    {
    }

    public ReadQuery getQuery(SelectStatement selectStatement, QueryOptions options, DataLimits limits, int nowInSec)
    {
        return ReadQuery.EMPTY;
    }
}
