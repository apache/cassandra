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

package org.apache.cassandra.service.accord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.junit.Test;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
import accord.primitives.PartialTxn;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Observable;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.CassandraGenerators;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static accord.utils.Property.qt;
import static org.apache.cassandra.config.DatabaseDescriptor.setSelectedSSTableFormat;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.setMemtable;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;
import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;

public class AccordKeyspaceTest extends CQLTester.InMemory
{
    static
    {
        // since this test does frequent truncates, the info table gets updated and forced flushed... which is 90% of the cost of this test...
        // this flag disables that flush
        CassandraRelevantProperties.UNSAFE_SYSTEM.setBoolean(true);
    }

    @Test
    public void serde()
    {
        AtomicLong now = new AtomicLong();

        String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
        TableId tableId = Schema.instance.getTableMetadata(KEYSPACE, tableName).id;
        Ranges scope = Ranges.of(new TokenRange(AccordRoutingKey.SentinelKey.min(tableId), AccordRoutingKey.SentinelKey.max(tableId)));

        AccordCommandStore store = AccordTestUtils.createAccordCommandStore(now::incrementAndGet, KEYSPACE, tableName);

        TxnId id = new TxnId(Timestamp.fromValues(1, 42, new Node.Id(1)), Txn.Kind.Read, Routable.Domain.Key);

        Txn txn = createTxn(wrapInTxn(String.format("SELECT * FROM %s.%s WHERE k=? LIMIT 1", KEYSPACE, tableName)), Collections.singletonList(42));

        PartialTxn partialTxn = txn.slice(scope, true);
        RoutingKey routingKey = partialTxn.keys().get(0).asKey().toUnseekable();
        FullRoute<?> route = partialTxn.keys().toRoute(routingKey);
        Deps deps = new Deps(KeyDeps.none((Keys) txn.keys()), RangeDeps.NONE, KeyDeps.NONE);

        CommonAttributes.Mutable common = new CommonAttributes.Mutable(id);
        common.partialTxn(partialTxn);
        common.route(route);
        common.partialDeps(deps.slice(scope));
        common.durability(Status.Durability.NotDurable);
        Command.WaitingOn waitingOn = null;

        Command.Committed committed = Command.SerializerSupport.committed(common, SaveStatus.Committed, id, Ballot.ZERO, Ballot.ZERO, waitingOn);
        AccordSafeCommand safeCommand = new AccordSafeCommand(AccordTestUtils.loaded(id, null));
        safeCommand.set(committed);

        AccordTestUtils.appendCommandsBlocking(store, null, committed);

        Mutation mutation = AccordKeyspace.getCommandMutation(store, safeCommand, 42);
        mutation.apply();

        Command loaded = store.loadCommand(id);
        Assertions.assertThat(loaded).isEqualTo(committed);
    }

    @Test
    public void findOverlappingKeys()
    {
        var tableIdGen = fromQT(CassandraGenerators.TABLE_ID_GEN);
        var partitionGen = fromQT(CassandraGenerators.partitioners());

        var sstableFormats = DatabaseDescriptor.getSSTableFormats();
        List<String> sstableFormatNames = new ArrayList<>(sstableFormats.keySet());
        sstableFormatNames.sort(Comparator.naturalOrder());

        List<String> memtableFormats = MemtableParams.knownDefinitions().stream()
                                                     .filter(name -> !name.startsWith("test_") && !name.equals("default"))
                                                     .sorted()
                                                     .collect(Collectors.toList());

        qt().check(rs -> {
            AccordKeyspace.unsafeClear();
            // control SSTable format
            setSelectedSSTableFormat(sstableFormats.get(rs.pick(sstableFormatNames)));
            // control memtable format
            setMemtable(ACCORD_KEYSPACE_NAME, "commands_for_key", rs.pick(memtableFormats));

            // define the tables w/ partitioners for the test
            // this uses the ability to override the SchemaProvider for the keyspace and only defines the single API call expected: getTablePartitioner
            TreeMap<TableId, IPartitioner> tables = new TreeMap<>();
            int numTables = rs.nextInt(1, 3);
            for (int i = 0; i < numTables; i++)
            {
                var tableId = tableIdGen.next(rs);
                while (tables.containsKey(tableId))
                    tableId = tableIdGen.next(rs);
                tables.put(tableId, partitionGen.next(rs));
            }
            SchemaProvider schema = Mockito.mock(SchemaProvider.class);
            Mockito.when(schema.getTablePartitioner(Mockito.any())).thenAnswer((Answer<IPartitioner>) invocationOnMock -> tables.get(invocationOnMock.getArgument(0)));
            AccordKeyspace.unsafeSetSchema(schema);

            int numStores = rs.nextInt(1, 3);

            // The model of the DB
            TreeMap<Integer, SortedSet<PartitionKey>> storesToKeys = new TreeMap<>();
            // write to the table and the model
            for (int i = 0, numKeys = rs.nextInt(10, 20); i < numKeys; i++)
            {
                int store = rs.nextInt(0, numStores);
                var keys = storesToKeys.computeIfAbsent(store, ignore -> new TreeSet<>());
                PartitionKey pk = null;
                // LocalPartitioner may have a type with a very small domain (boolean, vector<boolean, 1>, etc.), so need to bound the attempts
                // else this will loop forever...
                for (int attempt = 0; attempt < 10; attempt++)
                {
                    TableId tableId = rs.pickOrderedSet(tables.navigableKeySet());
                    IPartitioner partitioner = tables.get(tableId);
                    ByteBuffer data = !(partitioner instanceof LocalPartitioner) ? Int32Type.instance.decompose(rs.nextInt())
                                                                                 : fromQT(getTypeSupport(partitioner.getTokenValidator()).bytesGen()).next(rs);
                    PartitionKey key = new PartitionKey(tableId, tables.get(tableId).decorateKey(data));
                    if (keys.add(key))
                    {
                        pk = key;
                        break;
                    }
                }
                if (pk != null)
                {
                    try
                    {
                        // using Mutation directly (what we do in Accord) can break when user data is too large; leading to data loss
                        // The memtable will allow the write, but it will be dropped when writing to the SSTable...
                        //TODO (now, correctness): since we store the user token + user key, if a key is close to the PK limits then we could tip over and loose our CFK
//                        new Mutation(AccordKeyspace.getCommandsForKeyPartitionUpdate(store, pk, 42, ByteBufferUtil.EMPTY_BYTE_BUFFER)).apply();
                        execute("INSERT INTO system_accord.commands_for_key (store_id, key_token, key) VALUES (?, ?, ?)",
                                store, AccordKeyspace.serializeRoutingKey(pk.toUnseekable()), AccordKeyspace.serializeKey(pk));
                    }
                    catch (IllegalArgumentException | InvalidRequestException e)
                    {
                        // Sometimes the types are too large (LocalPartitioner) so the mutation gets rejected... just ignore those cases
                        // Length 69912 > max length 65535
                        String msg = e.getMessage();
                        if (msg != null)
                        {
                            if ((msg.startsWith("Length ") && msg.endsWith("> max length 65535")) // Clustering was rejected
                                || (msg.startsWith("Key length of ") && msg.endsWith(" is longer than maximum of 65535"))) // Partition was rejected
                            {
                                // failed to add
                                keys.remove(pk);
                                continue;
                            }
                        }
                        throw e;
                    }
                }
            }

            // read from the table and validate it matches the model
            for (int read = 0; read < 2; read++) // read=0 is memtable, read=1 is sstable
            {
                {
                    // Make sure no data was lost
                    // An issue was found that system mutations bypass checks so make their way to the Memtable, but when we flush to SSTable
                    // they get filtered out, causing data loss... This check is here to make sure that the data is present (test covers Memtable + SStable)
                    // in the storage before checking if the filtering logic is correct
                    TreeMap<Integer, SortedSet<ByteBuffer>> expectedCqlStoresToKeys = new TreeMap<>();
                    for (var e : storesToKeys.entrySet())
                    {
                        int store = e.getKey();
                        expectedCqlStoresToKeys.put(store, new TreeSet<>(e.getValue().stream().map(p -> AccordKeyspace.serializeRoutingKey(p.toUnseekable())).collect(Collectors.toList())));
                    }

                    // make sure no data loss... when this test was written sstable had all the rows but the sstable didn't... this
                    // is mostly a santity check to detect that case early
                    var resultSet = execute("SELECT store_id, key_token FROM system_accord.commands_for_key ALLOW FILTERING");
                    TreeMap<Integer, SortedSet<ByteBuffer>> cqlStoresToKeys = new TreeMap<>();
                    for (var row : resultSet)
                    {
                        int storeId = row.getInt("store_id");
                        ByteBuffer bb = row.getBytes("key_token");
                        cqlStoresToKeys.computeIfAbsent(storeId, ignore -> new TreeSet<>()).add(bb);
                    }
                    Assertions.assertThat(cqlStoresToKeys).isEqualTo(expectedCqlStoresToKeys);
                }

                for (int i = 0, queries = rs.nextInt(1, 5); i < queries; i++)
                {
                    int store = rs.pickOrderedSet(storesToKeys.navigableKeySet());
                    var keysForStore = new ArrayList<>(storesToKeys.get(store));

                    int offset;
                    int offsetEnd;
                    if (keysForStore.size() == 1)
                    {
                        offset = 0;
                        offsetEnd = 1;
                    }
                    else
                    {
                        offset = rs.nextInt(0, keysForStore.size());
                        offsetEnd = rs.nextInt(offset, keysForStore.size()) + 1;
                    }
                    List<PartitionKey> expected = keysForStore.subList(offset, offsetEnd);
                    PartitionKey start = expected.get(0);
                    PartitionKey end = expected.get(expected.size() - 1);

                    AsyncChain<List<PartitionKey>> map = Observable.asChain(callback -> AccordKeyspace.findAllKeysBetween(store, start.toUnseekable(), true, end.toUnseekable(), true, callback));
                    List<PartitionKey> actual = AsyncChains.getUnchecked(map);
                    Assertions.assertThat(actual).isEqualTo(expected);
                }

                if (read == 0)
                    Keyspace.open(ACCORD_KEYSPACE_NAME).getColumnFamilyStore("commands_for_key").forceBlockingFlush(UNIT_TESTS);
            }
        });
    }
}