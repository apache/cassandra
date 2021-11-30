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

import java.io.IOException;

import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Some subset of data to be streamed. Implementations handle writing out their data via the write method.
 * On the receiving end, {@link IncomingStream} streams the data in.
 *
 * All the data contained in a given stream needs to have the same repairedAt timestamp (or 0) and pendingRepair
 * id (or null).
 */
public interface OutgoingStream
{
    /**
     * Write the streams data into the socket
     */
    void write(StreamSession session, StreamingDataOutputPlus output, int version) throws IOException;

    /**
     * Release any resources held by the stream
     */
    void finish();

    long getRepairedAt();
    TimeUUID getPendingRepair();

    String getName();

    /**
     * @return estimated file size to be streamed. This should only be used for metrics, because concurrent
     * stats metadata update and index redistribution will change file sizes.
     */
    long getEstimatedSize();
    TableId getTableId();
    int getNumFiles();
}
