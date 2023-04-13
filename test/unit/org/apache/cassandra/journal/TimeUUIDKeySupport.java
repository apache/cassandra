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
package org.apache.cassandra.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.FBUtilities.updateChecksumLong;

class TimeUUIDKeySupport implements KeySupport<TimeUUID>
{
    static final TimeUUIDKeySupport INSTANCE = new TimeUUIDKeySupport();

    @Override
    public int serializedSize(int userVersion)
    {
        return 16;
    }

    @Override
    public void serialize(TimeUUID key, DataOutputPlus out, int userVersion) throws IOException
    {
        out.writeLong(key.uuidTimestamp());
        out.writeLong(key.lsb());
    }

    @Override
    public TimeUUID deserialize(DataInputPlus in, int userVersion) throws IOException
    {
        long uuidTimestamp = in.readLong();
        long lsb = in.readLong();
        return new TimeUUID(uuidTimestamp, lsb);
    }

    @Override
    public TimeUUID deserialize(ByteBuffer buffer, int position, int userVersion)
    {
        long uuidTimestamp = buffer.getLong(position);
        long lsb = buffer.getLong(position + 8);
        return new TimeUUID(uuidTimestamp, lsb);
    }

    @Override
    public void updateChecksum(Checksum crc, TimeUUID key, int userVersion)
    {
        updateChecksumLong(crc, key.uuidTimestamp());
        updateChecksumLong(crc, key.lsb());
    }

    @Override
    public int compareWithKeyAt(TimeUUID key, ByteBuffer buffer, int position, int userVersion)
    {
        long uuidTimestamp = buffer.getLong(position);
        long lsb = buffer.getLong(position + 8);
        return key.uuidTimestamp() != uuidTimestamp
             ? Long.compare(key.uuidTimestamp(), uuidTimestamp)
             : Long.compare(key.lsb(), lsb);
    }

    @Override
    public int compare(TimeUUID o1, TimeUUID o2)
    {
        return o1.compareTo(o2);
    }
}
