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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class UnreconciledMutationsTest
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";
    private static TableId TABLE_ID;

    @BeforeClass
    public static void setUp() throws IOException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(3),
                                    TableMetadata.builder(KEYSPACE, TABLE)
                                                 .addPartitionKeyColumn("k", Int32Type.instance)
                                                 .addRegularColumn("v", Int32Type.instance)
                                                 .build());
        TABLE_ID = Schema.instance.getTableMetadata(KEYSPACE, TABLE).id;
    }

    private static Token tokenFor(int key)
    {
        return new ByteOrderedPartitioner.BytesToken(ByteBufferUtil.bytes(key));
    }

    private static Mutation createMutation(int partitionKey, int value, int offset)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        
        // Create a MutationId with logId 1L and sequenceId that produces the desired offset
        MutationId mutationId = new MutationId(CoordinatorLogId.asLong(1, 1), MutationId.sequenceId(offset, 0));
        
        Mutation mutation = new RowUpdateBuilder(metadata, 0, partitionKey)
                           .add("v", value)
                           .build()
                           .withMutationId(mutationId);
        return mutation;
    }

    private static void addToOffsets(Offsets.OffsetReciever receiver, int... offsets)
    {
        for (int offset : offsets)
            receiver.add(offset);
    }

    @Test
    public void testSingleTokenCollectionIsolation()
    {
        UnreconciledMutations unreconciled = new UnreconciledMutations();

        // Create mutations for different partition keys with different tokens
        Mutation mutation1 = createMutation(100, 1000, 1);
        Mutation mutation2 = createMutation(200, 2000, 2);
        Mutation mutation3 = createMutation(300, 3000, 3);
        
        Token token1 = mutation1.key().getToken();
        Token token2 = mutation2.key().getToken();
        Token token3 = mutation3.key().getToken();
        
        // Add all mutations to unreconciled state
        unreconciled.startWriting(mutation1);
        unreconciled.startWriting(mutation2);
        unreconciled.startWriting(mutation3);
        
        // Make them visible
        unreconciled.finishWriting(mutation1);
        unreconciled.finishWriting(mutation2);
        unreconciled.finishWriting(mutation3);
        
        // Test single token collection for token1 - should ONLY return mutation1
        Offsets.Mutable offsets1 = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean found1 = unreconciled.collect(token1, TABLE_ID, false, offsets1);
        
        Assert.assertTrue("Should find mutations for token1", found1);
        assertEquals("Should only contain 1 offset for token1", 1, offsets1.offsetCount());
        Assert.assertTrue("Should contain offset 1 for mutation1", offsets1.contains(1));
        Assert.assertFalse("Should NOT contain offset 2 for mutation2", offsets1.contains(2));
        Assert.assertFalse("Should NOT contain offset 3 for mutation3", offsets1.contains(3));
        
        // Test single token collection for token2 - should ONLY return mutation2
        Offsets.Mutable offsets2 = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean found2 = unreconciled.collect(token2, TABLE_ID, false, offsets2);
        
        Assert.assertTrue("Should find mutations for token2", found2);
        assertEquals("Should only contain 1 offset for token2", 1, offsets2.offsetCount());
        Assert.assertTrue("Should contain offset 2 for mutation2", offsets2.contains(2));
        Assert.assertFalse("Should NOT contain offset 1 for mutation1", offsets2.contains(1));
        Assert.assertFalse("Should NOT contain offset 3 for mutation3", offsets2.contains(3));
        
        // Test single token collection for token3 - should ONLY return mutation3
        Offsets.Mutable offsets3 = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean found3 = unreconciled.collect(token3, TABLE_ID, false, offsets3);
        
        Assert.assertTrue("Should find mutations for token3", found3);
        assertEquals("Should only contain 1 offset for token3", 1, offsets3.offsetCount());
        Assert.assertTrue("Should contain offset 3 for mutation3", offsets3.contains(3));
        Assert.assertFalse("Should NOT contain offset 1 for mutation1", offsets3.contains(1));
        Assert.assertFalse("Should NOT contain offset 2 for mutation2", offsets3.contains(2));
    }

    @Test
    public void testEmptyCollection()
    {
        UnreconciledMutations unreconciled = new UnreconciledMutations();
        Token token = tokenFor(100);
        
        Offsets.Mutable offsets = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean found = unreconciled.collect(token, TABLE_ID, false, offsets);
        
        Assert.assertFalse("Should not find any mutations in empty collection", found);
        Assert.assertTrue("Should have no offsets", offsets.isEmpty());
    }

    @Test
    public void testPendingVsVisibleMutations()
    {
        UnreconciledMutations unreconciled = new UnreconciledMutations();

        Mutation pendingMutation = createMutation(100, 1000, 1);
        Mutation visibleMutation = createMutation(100, 2000, 2);
        Token token = pendingMutation.key().getToken();
        
        // Add one pending, one visible
        unreconciled.startWriting(pendingMutation);
        unreconciled.startWriting(visibleMutation);
        unreconciled.finishWriting(visibleMutation); // Only make the second visible
        
        // Test without including pending - should only get visible mutation
        Offsets.Mutable visibleOnly = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean foundVisible = unreconciled.collect(token, TABLE_ID, false, visibleOnly);
        
        Assert.assertTrue("Should find visible mutations", foundVisible);
        assertEquals("Should only have 1 visible mutation", 1, visibleOnly.offsetCount());
        Assert.assertTrue("Should contain visible mutation offset", visibleOnly.contains(2));
        Assert.assertFalse("Should NOT contain pending mutation offset", visibleOnly.contains(1));
        
        // Test including pending - should get both mutations
        Offsets.Mutable includingPending = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean foundAll = unreconciled.collect(token, TABLE_ID, true, includingPending);
        
        Assert.assertTrue("Should find all mutations", foundAll);
        assertEquals("Should have 2 mutations total", 2, includingPending.offsetCount());
        Assert.assertTrue("Should contain pending mutation offset", includingPending.contains(1));
        Assert.assertTrue("Should contain visible mutation offset", includingPending.contains(2));
    }

    @Test
    public void testMultipleMutationsSameToken()
    {
        UnreconciledMutations unreconciled = new UnreconciledMutations();

        // Create multiple mutations for the same partition key (same token)
        Mutation mutation1 = createMutation(100, 1000, 1);
        Mutation mutation2 = createMutation(100, 2000, 2);
        Mutation mutation3 = createMutation(100, 3000, 3);
        Token token = mutation1.key().getToken();
        
        unreconciled.startWriting(mutation1);
        unreconciled.startWriting(mutation2);
        unreconciled.startWriting(mutation3);
        
        unreconciled.finishWriting(mutation1);
        unreconciled.finishWriting(mutation2);
        unreconciled.finishWriting(mutation3);
        
        Offsets.Mutable offsets = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean found = unreconciled.collect(token, TABLE_ID, false, offsets);
        
        Assert.assertTrue("Should find mutations for token", found);
        assertEquals("Should have all 3 mutations for same token", 3, offsets.offsetCount());
        Assert.assertTrue("Should contain offset 1", offsets.contains(1));
        Assert.assertTrue("Should contain offset 2", offsets.contains(2));
        Assert.assertTrue("Should contain offset 3", offsets.contains(3));
    }

    @Test
    public void testTableIdFiltering()
    {
        UnreconciledMutations unreconciled = new UnreconciledMutations();

        // Create a fake different table ID
        TableId differentTableId = TableId.generate();
        
        Mutation mutation = createMutation(100, 1000, 1);
        Token token = mutation.key().getToken();
        
        unreconciled.startWriting(mutation);
        unreconciled.finishWriting(mutation);
        
        // Query with correct table ID - should find mutation
        Offsets.Mutable correctTable = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean foundCorrect = unreconciled.collect(token, TABLE_ID, false, correctTable);
        
        Assert.assertTrue("Should find mutation for correct table", foundCorrect);
        assertEquals("Should have 1 mutation", 1, correctTable.offsetCount());
        
        // Query with different table ID - should find nothing
        Offsets.Mutable differentTable = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean foundDifferent = unreconciled.collect(token, differentTableId, false, differentTable);
        
        Assert.assertFalse("Should not find mutation for different table", foundDifferent);
        Assert.assertTrue("Should have no mutations", differentTable.isEmpty());
    }

    @Test
    public void testMutationRemoval()
    {
        UnreconciledMutations unreconciled = new UnreconciledMutations();

        Mutation mutation = createMutation(100, 1000, 1);
        Token token = mutation.key().getToken();
        
        unreconciled.startWriting(mutation);
        unreconciled.finishWriting(mutation);
        
        // Verify mutation is present
        Offsets.Mutable beforeRemoval = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean foundBefore = unreconciled.collect(token, TABLE_ID, false, beforeRemoval);
        Assert.assertTrue("Should find mutation before removal", foundBefore);
        assertEquals("Should have 1 mutation before removal", 1, beforeRemoval.offsetCount());
        
        // Remove the mutation
        unreconciled.remove(1);
        
        // Verify mutation is gone
        Offsets.Mutable afterRemoval = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean foundAfter = unreconciled.collect(token, TABLE_ID, false, afterRemoval);
        Assert.assertFalse("Should not find mutation after removal", foundAfter);
        Assert.assertTrue("Should have no mutations after removal", afterRemoval.isEmpty());
    }

    @Test
    public void testTokenRangeCollection()
    {
        UnreconciledMutations unreconciled = new UnreconciledMutations();

        // Create mutations with different tokens and sort them
        List<Integer> keys = List.of(100, 200, 300, 400, 500);
        List<Mutation> mutations = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++)
        {
            Mutation mutation = createMutation(keys.get(i), keys.get(i) * 10, i + 1);
            mutations.add(mutation);
            unreconciled.startWriting(mutation);
            unreconciled.finishWriting(mutation);
        }
        
        // Sort mutations by token for predictable range testing
        mutations.sort((m1, m2) -> m1.key().getToken().compareTo(m2.key().getToken()));
        
        Token firstToken = mutations.get(0).key().getToken();
        Token middleToken = mutations.get(2).key().getToken();
        Token lastToken = mutations.get(4).key().getToken();
        
        // Test range from first to middle (inclusive)
        DecoratedKey startKey = mutations.get(0).key();
        DecoratedKey endKey = mutations.get(2).key();
        Range<Token> range = 
            new Range<>(firstToken, middleToken);
        AbstractBounds<PartitionPosition> bounds = 
            new Bounds<>(startKey, endKey);
        
        Offsets.Mutable rangeOffsets = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean foundRange = unreconciled.collect(bounds, TABLE_ID, false, rangeOffsets);
        
        Assert.assertTrue("Should find mutations in range", foundRange);
        // Should include mutations at positions 0, 1, and 2 (first through middle inclusive)
        Assert.assertTrue("Range should be inclusive of boundaries", rangeOffsets.offsetCount() >= 3);
    }

    @Test
    public void testSingleTokenRangeCollectionBug()
    {
        UnreconciledMutations unreconciled = new UnreconciledMutations();

        // Create mutations for different partition keys with different tokens
        Mutation mutation1 = createMutation(100, 1000, 1);
        Mutation mutation2 = createMutation(200, 2000, 2);
        Mutation mutation3 = createMutation(300, 3000, 3);
        
        // Add all mutations to unreconciled state
        unreconciled.startWriting(mutation1);
        unreconciled.startWriting(mutation2);
        unreconciled.startWriting(mutation3);
        
        // Make them visible
        unreconciled.finishWriting(mutation1);
        unreconciled.finishWriting(mutation2);
        unreconciled.finishWriting(mutation3);
        
        // Test single token range collection
        Offsets.Mutable offsets = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        Bounds<PartitionPosition> bounds = new Bounds<>(mutation1.key(), mutation1.key());
        boolean found = unreconciled.collect(bounds, TABLE_ID, false, offsets);
        
        // This should only return mutation1
        Assert.assertTrue("Should find mutations for single token range", found);
        assertEquals("Single token range should only return 1 mutation", 1, offsets.offsetCount());
        Assert.assertTrue("Should contain mutation1 offset", offsets.contains(1));

        // other mutations should not be included
        Assert.assertFalse("Single token range collection should NOT contain mutation2", offsets.contains(2));
        Assert.assertFalse("Single token range collection should NOT contain mutation3", offsets.contains(3));
    }

    @Test
    public void testFullRangeCollectionWithMinimumToken()
    {
        UnreconciledMutations unreconciled = new UnreconciledMutations();

        // Create mutations for different partition keys with different tokens
        Mutation mutation1 = createMutation(100, 1000, 1);
        Mutation mutation2 = createMutation(200, 2000, 2);
        Mutation mutation3 = createMutation(300, 3000, 3);
        
        // Add all mutations to unreconciled state
        unreconciled.startWriting(mutation1);
        unreconciled.startWriting(mutation2);
        unreconciled.startWriting(mutation3);
        
        // Make them visible
        unreconciled.finishWriting(mutation1);
        unreconciled.finishWriting(mutation2);
        unreconciled.finishWriting(mutation3);
        
        // Create a full-range query using minimum token pattern (same start/end with minimum token)
        // This is the legitimate case where cmp == 0 should mean "full range"
        Token minimumToken = DatabaseDescriptor.getPartitioner().getMinimumToken();
        Range<PartitionPosition> fullRange = new Range<>(minimumToken.minKeyBound(), minimumToken.minKeyBound());

        // Test full range collection - this SHOULD return ALL mutations
        Offsets.Mutable offsets = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean found = unreconciled.collect(fullRange, TABLE_ID, false, offsets);
        
        // Full range should return ALL mutations
        Assert.assertTrue("Should find mutations for full range", found);
        assertEquals("Full range should return ALL mutations", 3, offsets.offsetCount());
        Assert.assertTrue("Should contain mutation1 offset", offsets.contains(1));
        Assert.assertTrue("Should contain mutation2 offset", offsets.contains(2));
        Assert.assertTrue("Should contain mutation3 offset", offsets.contains(3));
    }

    @Test
    public void testSingleTokenCollectionVsFullRange()
    {
        UnreconciledMutations unreconciled = new UnreconciledMutations();

        // Create mutations with different tokens
        Mutation mutation1 = createMutation(100, 1000, 1);
        Mutation mutation2 = createMutation(200, 2000, 2);
        Mutation mutation3 = createMutation(300, 3000, 3);
        
        Token targetToken = mutation1.key().getToken();
        
        unreconciled.startWriting(mutation1);
        unreconciled.startWriting(mutation2);
        unreconciled.startWriting(mutation3);
        
        unreconciled.finishWriting(mutation1);
        unreconciled.finishWriting(mutation2);
        unreconciled.finishWriting(mutation3);
        
        // Single token collection should only return mutation1
        Offsets.Mutable singleToken = new Offsets.Mutable(new CoordinatorLogId(1, 1));
        boolean foundSingle = unreconciled.collect(targetToken, TABLE_ID, false, singleToken);
        
        Assert.assertTrue("Should find mutation for single token", foundSingle);
        assertEquals("Single token should only return 1 mutation", 1, singleToken.offsetCount());
        Assert.assertTrue("Should contain mutation1 offset", singleToken.contains(1));
        
        // This is the CRITICAL test - single token collection must NOT return all mutations
        // If the bug exists, this test will fail because it returns all 3 mutations instead of 1
        Assert.assertFalse("Single token collection should NOT contain mutation2", singleToken.contains(2));
        Assert.assertFalse("Single token collection should NOT contain mutation3", singleToken.contains(3));
    }

    @Test
    public void testLoadFromJournal()
    {
        MutationJournal.start();

        CoordinatorLogId logId = new CoordinatorLogId(1, 1);

        Offsets.Mutable offsets1 = new Offsets.Mutable(logId);
        offsets1.add(1, 2, 3, 4, 5, 6, 7);

        Offsets.Mutable offsets2 = new Offsets.Mutable(logId);
        offsets2.add(1, 2, 3, 4, 5);

        Node2OffsetsMap witnessed = new Node2OffsetsMap();
        witnessed.set(1, offsets1);
        witnessed.set(2, offsets2);

        Mutation mutation6 = createMutation(6, 6, 6);
        Mutation mutation7 = createMutation(7, 7, 7);
        MutationJournal.instance().write(mutation6.id(), mutation6);
        MutationJournal.instance().write(mutation7.id(), mutation7);

        Offsets.Mutable loadedOffsets = new Offsets.Mutable(logId);
        UnreconciledMutations unreconciled = UnreconciledMutations.loadFromJournal(witnessed, 1);
        unreconciled.collect(mutation6.key().getToken(), TABLE_ID, false, loadedOffsets);
        unreconciled.collect(mutation7.key().getToken(), TABLE_ID, false, loadedOffsets);
        assertEquals(List.of(6, 7), loadedOffsets.asList());
    }
}