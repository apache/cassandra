/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.cassandra.index.sai;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

import static junit.framework.TestCase.assertEquals;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.index.sai.SAITester.vector;


public class StorageAttachedIndexTest
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tab";
    private static final int DIMENSION = 2;
    private static final int numSSTables = 2;
    private ColumnFamilyStore cfs;
    private List<ByteBuffer> byteBufferList;
    private List<CQLTester.Vector<Float>> vectorFloatList;
    private SingleColumnRestriction.AnnRestriction testRestriction;
    private StorageAttachedIndex sai;

    @Before
    public void setup() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization(() -> {
            Config config = DatabaseDescriptor.loadConfig();
            config.partitioner = Murmur3Partitioner.class.getName();
            return config;
        });

        SchemaLoader.prepareServer();
        Gossiper.instance.maybeInitializeLocalState(0);
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
        QueryProcessor.executeInternal(String.format("CREATE TABLE %s.%s (key int primary key, value vector<float, %s>)", KEYSPACE, TABLE, DIMENSION));
        QueryProcessor.executeInternal(String.format("CREATE CUSTOM INDEX ON %s.%s(value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function': 'dot_product'}", KEYSPACE, TABLE));

        vectorFloatList = new ArrayList<>();
        CQLTester.Vector<Float> vector1 = vector(1, 2);
        CQLTester.Vector<Float> vector2 = vector(3, 4);
        CQLTester.Vector<Float> vector3 = vector(5, 6);
        vectorFloatList.add(vector1);
        vectorFloatList.add(vector2);
        vectorFloatList.add(vector3);

        cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);

        for (int i = 0; i < numSSTables ; i++)
        {
            for (CQLTester.Vector<Float> vectorFloat : vectorFloatList)
            {
                QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, value) VALUES (?, ?)", KEYSPACE, TABLE), i, vectorFloat);
            }
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }

        TableMetadata tableMetadata = cfs.metadata();
        String columnName = "value";
        ColumnIdentifier columnIdentifier = new ColumnIdentifier(columnName, true);
        ColumnMetadata columnDef = tableMetadata.getExistingColumn(columnIdentifier);
        if (!(columnDef.type instanceof VectorType))
            throw invalidRequest("ANN is only supported against DENSE FLOAT32 columns");

        // Convert List<CQLTester.Vector<Float>> to List<ByteBuffer>
        byteBufferList = new ArrayList<>();
        for (CQLTester.Vector<Float> vector : vectorFloatList) {
            ByteBuffer buffer = floatVectorToByteBuffer(vector);
            byteBufferList.add(buffer);
        }

        Term terms = new Lists.Value(byteBufferList);

        testRestriction = new SingleColumnRestriction.AnnRestriction(columnDef, terms);

        sai = (StorageAttachedIndex) cfs.getIndexManager().getIndexByName(String.format("%s_value_idx", TABLE));
    }

    private static ByteBuffer floatVectorToByteBuffer(CQLTester.Vector<Float> vector) {
        ByteBuffer buffer = ByteBuffer.allocate(vector.size() * 4);  // 4 bytes per float
        for (Float value : vector) {
            buffer.putFloat(value);
        }
        buffer.flip();
        return buffer;
    }

    @Test
    public void testOrderResults() {
        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(new ArrayList<>(cfs.metadata.get().columns()));
        QueryOptions queryOptions = QueryOptions.create(ConsistencyLevel.ONE,
                                                        byteBufferList,
                                                        false,
                                                        PageSize.inRows(1),
                                                        null,
                                                        null,
                                                        ProtocolVersion.CURRENT,
                                                        KEYSPACE);
        List<List<ByteBuffer>> rows = new ArrayList<>();
        rows.add(byteBufferList);
        ResultSet resultSet = new ResultSet(resultMetadata, rows);

        SelectStatement selectStatementInstance = (SelectStatement) QueryProcessor.prepareInternal("SELECT key, value FROM " + KEYSPACE + '.' + TABLE).statement;
        selectStatementInstance.orderResults(resultSet, queryOptions);

        List<List<ByteBuffer>> sortedRows = resultSet.rows;

        Comparator<List<ByteBuffer>> descendingComparator = (o1, o2) -> {
            ByteBuffer value1 = o1.get(0);
            ByteBuffer value2 = o2.get(0);
            return value2.compareTo(value1);
        };

        Collections.sort(rows, descendingComparator);

        for (int i = 0; i < sortedRows.size(); i++) {
            List<ByteBuffer> expectedRow = rows.get(i);
            List<ByteBuffer> actualRow = sortedRows.get(i);
            assertEquals(expectedRow, actualRow);
        }
    }
}
