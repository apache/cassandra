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
package org.apache.cassandra.index.sai.cql;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Row;
import org.apache.cassandra.Util;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class TokenCollisionTest extends SAITester
{
    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void testSkippingWhenTokensCollide() throws Throwable
    {
        createTable("CREATE TABLE %s (pk blob, value text, PRIMARY KEY (pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        ByteBuffer prefix = ByteBufferUtil.bytes("key");
        final int numRows = 640; // 5 blocks x 128 postings, so skip table will contain 5 entries
        for (int i = 0; i < numRows; i++)
        {
            ByteBuffer pk = Util.generateMurmurCollision(prefix, (byte) (i / 64), (byte) (i % 64));
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", pk, "abc");
        }
        // sanity check, all should have the same partition key
        assertEquals(numRows, execute("SELECT pk FROM %s WHERE token(pk) = token(?)", prefix).size());
        flush();

        // A storage-attached index will advance token flow to the token that is shared between all indexed rows,
        // and cause binary search on the postings skip table that looks like this [3, 3, 3, 3, 3].
        List<Row> rows = executeNet("SELECT * FROM %s WHERE token(pk) >= token(?) AND value = 'abc'", prefix).all();
        // we should match all the rows
        assertEquals(numRows, rows.size());
    }
}
