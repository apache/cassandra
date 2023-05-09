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

package org.apache.cassandra.service.reads;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.locator.Replica;

/**
 * Execute followup read commands, used for short read protection or read repair
 * Having an interface allows followup reads to be routed through a transaction
 * system ensuring the reads are done safely
 */
public interface CassandraFollowupReader
{
    default void read(ReadCommand command, Replica replica, ReadCallback callback)
    {
        read(command, replica, callback, false);
    }

    void read(ReadCommand command, Replica replica, ReadCallback callback, boolean trackRepairedStatus);
}
