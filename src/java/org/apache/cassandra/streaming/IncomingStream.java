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

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.TableId;

/**
 * The counterpart of {@link OutgoingStream} on the receiving side.
 *
 * Data streamed in can (and should) be persisted, but must not be included in the table's
 * live data set until added by {@link StreamReceiver}. If the stream fails, the stream receiver will
 * delete the streamed data, but implementations still need to handle the case where it's process dies
 * during streaming, and it has data left around on startup, in which case it should be deleted.
 */
public interface IncomingStream
{

    /**
     * Read in the stream data.
     */
    void read(DataInputPlus inputPlus, int version) throws Throwable;

    String getName();
    long getSize();
    int getNumFiles();
    TableId getTableId();

    /**
     * @return stream session used to receive given file
     */
    StreamSession session();
}
