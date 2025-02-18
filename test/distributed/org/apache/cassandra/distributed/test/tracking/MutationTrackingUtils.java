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

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

import org.junit.Assert;

import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;

public class MutationTrackingUtils
{
    private static final int VERSION = MessagingService.current_version;
    public static byte[] encodeId(MutationId id)
    {
        int size = Ints.checkedCast(MutationId.serializer.serializedSize(id, VERSION));
        ByteBuffer buffer = ByteBuffer.allocate(size);
        try (DataOutputBuffer dob = new DataOutputBuffer(buffer))
        {
            MutationId.serializer.serialize(id, dob, VERSION);
            return buffer.array();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static MutationId decodeId(byte[] bytes)
    {
        try (DataInputBuffer dib = new DataInputBuffer(bytes))
        {
            MutationId id = MutationId.serializer.deserialize(dib, VERSION);
            Assert.assertEquals(MutationId.serializer.serializedSize(id, VERSION), bytes.length);
            return id;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
