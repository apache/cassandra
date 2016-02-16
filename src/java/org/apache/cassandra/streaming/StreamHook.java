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

package org.apache.cassandra.streaming;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.utils.FBUtilities;

public interface StreamHook
{
    public static final StreamHook instance = createHook();

    public OutgoingFileMessage reportOutgoingFile(StreamSession session, SSTableReader sstable, OutgoingFileMessage message);
    public void reportStreamFuture(StreamSession session, StreamResultFuture future);
    public void reportIncomingFile(ColumnFamilyStore cfs, SSTableMultiWriter writer, StreamSession session, int sequenceNumber);

    static StreamHook createHook()
    {
        String className =  System.getProperty("cassandra.stream_hook");
        if (className != null)
        {
            return FBUtilities.construct(className, StreamHook.class.getSimpleName());
        }
        else
        {
            return new StreamHook()
            {
                public OutgoingFileMessage reportOutgoingFile(StreamSession session, SSTableReader sstable, OutgoingFileMessage message)
                {
                    return message;
                }

                public void reportStreamFuture(StreamSession session, StreamResultFuture future) {}

                public void reportIncomingFile(ColumnFamilyStore cfs, SSTableMultiWriter writer, StreamSession session, int sequenceNumber) {}
            };
        }
    }
}
