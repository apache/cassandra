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

package org.apache.cassandra.service.accord;

import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Key;
import accord.impl.SafeCommandsForKey;
import accord.primitives.RoutableKey;
import accord.utils.async.AsyncChain;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer;

public class AccordSafeCommandsForKeyUpdate extends SafeCommandsForKey.Update<CommandsForKeyUpdate, ByteBuffer> implements AccordSafeState<RoutableKey, CommandsForKeyUpdate>
{
    private boolean invalidated;
    private final AccordCachingState<RoutableKey, CommandsForKeyUpdate> global;
    private CommandsForKeyUpdate original;
    private CommandsForKeyUpdate current;

    public AccordSafeCommandsForKeyUpdate(AccordCachingState<RoutableKey, CommandsForKeyUpdate> global)
    {
        super((Key) global.key(), CommandsForKeySerializer.loader);
        this.global = global;
        this.original = null;
        this.current = null;
    }

    @Override
    public void initialize()
    {
        set(CommandsForKeyUpdate.empty((PartitionKey) key()));
    }

    @Override
    public AccordCachingState<RoutableKey, CommandsForKeyUpdate> global()
    {
        checkNotInvalidated();
        return global;
    }

    @Override
    public CommandsForKeyUpdate current()
    {
        checkNotInvalidated();
        return current;
    }

    public AsyncChain<?> loading()
    {
        throw new IllegalStateException("Updates aren't loaded");
    }

    @Override
    @VisibleForTesting
    public void set(CommandsForKeyUpdate cfk)
    {
        checkNotInvalidated();
        this.current = cfk;
    }

    public CommandsForKeyUpdate original()
    {
        checkNotInvalidated();
        return original;
    }

    public CommandsForKeyUpdate setUpdates()
    {
        CommandsForKeyUpdate next = new CommandsForKeyUpdate((PartitionKey) key(),
                                                             deps().toImmutable(),
                                                             all().toImmutable(),
                                                             common().toImmutable());
        set(next);
        return next;
    }

    @Override
    public void preExecute()
    {
        checkNotInvalidated();
        original = global.get();
        current = original;
    }

    @Override
    public void postExecute()
    {
        checkNotInvalidated();
        global.set(current);
    }

    @Override
    public void invalidate()
    {
        invalidated = true;
    }

    @Override
    public boolean invalidated()
    {
        return invalidated;
    }
}
