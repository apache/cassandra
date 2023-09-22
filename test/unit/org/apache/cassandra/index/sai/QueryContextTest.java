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

package org.apache.cassandra.index.sai;

import java.util.PriorityQueue;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.index.sai.utils.AbstractPrimaryKeyTest;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.ScoredPrimaryKey;

public class QueryContextTest extends AbstractPrimaryKeyTest
{

    @Test
    public void testQueryContextRecordScore()
    {
        QueryContext context = new QueryContext();

        PrimaryKey key1 = SAITester.TEST_FACTORY.create(Util.dk("key1"), Clustering.EMPTY);
        PrimaryKey key2 = SAITester.TEST_FACTORY.create(Util.dk("key2"), Clustering.EMPTY);

        context.recordScore(key1, 1.0f);
        assertEquals(1.0f, context.getScoreForKey(key1), 0.001f);

        // Record a different score for a different key
        context.recordScore(key2, 0.0f);

        // Update key1 to show it removes the value from the map
        context.recordScore(key1, 0.5f);

        assertEquals(-1.0f, context.getScoreForKey(key1), 0.001f);
        assertEquals(0.0f, context.getScoreForKey(key2), 0.001f);

        PrimaryKey unknown = SAITester.TEST_FACTORY.create(Util.dk("key3"), Clustering.EMPTY);
        // Missing key also returns -1.0f
        assertEquals(-1.0f, context.getScoreForKey(unknown), 0.001f);
    }

    @Test
    public void testQueryContextRecordScores()
    {
        QueryContext context = new QueryContext();
        PriorityQueue<PrimaryKey> queue = new PriorityQueue<>();
        for (int i = 0; i < 100; i++)
        {
            PrimaryKey key = SAITester.TEST_FACTORY.create(Util.dk("key" + i), Clustering.EMPTY);
            queue.add(ScoredPrimaryKey.create(key, 0.01f * i));
        }

        // Record all the scores
        context.recordScores(queue);

        // Check that all the scores are recorded
        for (int i = 0; i < 100; i++)
        {
            PrimaryKey key = SAITester.TEST_FACTORY.create(Util.dk("key" + i), Clustering.EMPTY);
            assertEquals(0.01f * i, context.getScoreForKey(key), 0.001f);
        }
    }
}
