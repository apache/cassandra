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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class ParticipateState extends AbstractCompletable<TimeUUID> implements WeightedHierarchy.Root
{
    public final InetAddressAndPort initiator;
    public final List<TableId> tableIds;
    public final Collection<Range<Token>> ranges;
    public final boolean incremental;
    public final long repairedAt;
    public final boolean global;
    public final PreviewKind previewKind;

    private final ConcurrentMap<UUID, ValidationState> validations = new ConcurrentHashMap<>();

    public final Phase phase = new Phase();

    private static final long EMPTY_SIZE = ObjectSizes.measure(new ParticipateState(null, new PrepareMessage(nextTimeUUID(), Collections.emptyList(), Collections.emptyList(), false, 0L, false, PreviewKind.NONE)));
    private final AtomicLong estimatedRetainedSize = new AtomicLong(0);

    @Override
    public long independentRetainedSize()
    {
        long size = EMPTY_SIZE;

        // initiator comes from the deserialized PrepareMessage, not TokenMetadata, so the reference retained by this class
        // is expected to be the sole reference to that instance.
        size += initiator == null ? 0 : initiator.unsharedHeapSize();
        for (TableId ignored : tableIds)
            size += TableId.EMPTY_SIZE;
        for (Range<Token> range : ranges)
            size += ObjectSizes.sizeOf(range);

        Result result = getResult();
        size += result == null ? 0 : result.unsharedHeapSize();

        return size;
    }

    @Override
    public WeightedHierarchy.Root root()
    {
        return this;
    }

    @Override
    public AtomicLong totalNestedRetainedSize()
    {
        return estimatedRetainedSize;
    }

    @Override
    public void onRetainedSizeUpdate()
    {
        ActiveRepairService.instance.onUpdate(this);
    }

    public ParticipateState(InetAddressAndPort initiator, PrepareMessage msg)
    {
        super(msg.parentRepairSession);
        this.initiator = initiator;
        this.tableIds = msg.tableIds;
        this.ranges = msg.ranges;
        this.incremental = msg.isIncremental;
        this.repairedAt = msg.repairedAt;
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

    public Collection<UUID> validationIds()
    {
        return validations.keySet();
    }

    public class Phase extends BasePhase
    {

    }
}
