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
package org.apache.cassandra.repair.state;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Throwables;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.TimeUUID;

public class ParticipateState
{
    public final Phase phase = new Phase();

    private final ConcurrentMap<UUID, ValidationState> validations = new ConcurrentHashMap<>();

    public final TimeUUID id;
    private final List<TableId> tableIds;
    private final Collection<Range<Token>> ranges;
    private final boolean incremental;
    private final long timestamp;
    private final boolean global;
    private final PreviewKind previewKind;

    public ParticipateState(PrepareMessage msg)
    {
        this.id = msg.parentRepairSession;
        this.tableIds = msg.tableIds;
        this.ranges = msg.ranges;
        this.incremental = msg.isIncremental;
        this.timestamp = msg.timestamp;
        this.global = msg.isGlobal;
        this.previewKind = msg.previewKind;
    }

    public boolean register(ValidationState state)
    {
        ValidationState current = validations.putIfAbsent(state.id, state);
        return current == null;
    }

    public Collection<ValidationState> validations()
    {
        return validations.values();
    }

    public class Phase
    {
        public void fail(Throwable e)
        {
            fail(e == null ? null : Throwables.getStackTraceAsString(e));
        }

        public void fail(String failureCause)
        {
            // TODO
        }
    }
}
