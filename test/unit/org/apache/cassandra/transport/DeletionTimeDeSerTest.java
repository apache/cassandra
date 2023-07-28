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
package org.apache.cassandra.transport;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;

import static org.junit.Assert.assertEquals;

/**
 * We test here serialization of DTs and some of it's sentinel values 
 *
 */
public class DeletionTimeDeSerTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }
    
    @Test
    public void testLDTDeserLive() throws IOException
    {
        assertEquals(DeletionTime.LIVE, serDeser(DeletionTime.LIVE));
    }
    
    @Test
    public void testLDTDeserLongMAX_VALUE() throws IOException
    {
        DeletionTime dt = DeletionTime.build(Long.MAX_VALUE, 2147483600);
        assertEquals(dt, serDeser(dt));
    }
    
    @Test
    public void testLDTDeser() throws IOException
    {
        // Test Serialization in all bit possible positions
        for (int i = 0; i < 63; i++)
        {
            DeletionTime dt = DeletionTime.build(1L << i, 2147483600);
            assertEquals(dt, serDeser(dt));
        }
    }
    
    private DeletionTime serDeser(DeletionTime dt) throws IOException
    {
        DeletionTime readDt = null;
        int offset = 2;

        try(DataOutputBuffer out = new DataOutputBuffer(12)) // Long + Int = 8 + 4
        {
            DeletionTime.getSerializer(BigFormat.getInstance().getLatestVersion()).serialize(dt, out);
            
            try(DataInputBuffer in = new DataInputBuffer(out.toByteArray()))
            {
                // Test *both* deserializers

                readDt = DeletionTime.getSerializer(BigFormat.getInstance().getLatestVersion()).deserialize(in);

                ByteBuffer bbOrig = out.buffer();
                bbOrig.rewind();
                ByteBuffer bb = ByteBuffer.allocate(bbOrig.capacity() + offset);
                bb.position(offset);
                bb.put(bbOrig);
                DeletionTime readDt2 = DeletionTime.getSerializer(BigFormat.getInstance().getLatestVersion()).deserialize(bb, offset);
                assertEquals(readDt, readDt2);
            }
        }
        
        return readDt;
    }
}
