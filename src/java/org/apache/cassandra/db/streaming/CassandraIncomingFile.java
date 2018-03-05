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

package org.apache.cassandra.db.streaming;

import java.io.IOException;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.streaming.IncomingStreamData;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;

public class CassandraIncomingFile implements IncomingStreamData
{
    private final ColumnFamilyStore cfs;
    private final StreamSession session;
    private final StreamMessageHeader header;

    public CassandraIncomingFile(ColumnFamilyStore cfs, StreamSession session, StreamMessageHeader header)
    {
        this.cfs = cfs;
        this.session = session;
        this.header = header;
    }

    @Override
    public void read(DataInputPlus in, int version) throws IOException
    {
        CassandraStreamHeader fileHeader = CassandraStreamHeader.serializer.deserialize(in, version);
        CassandraStreamReader reader = !fileHeader.isCompressed() ? new CassandraStreamReader(header, session)
                                                                  : new CompressedCassandraStreamReader(header, session);
        reader.read(in);
    }
}
