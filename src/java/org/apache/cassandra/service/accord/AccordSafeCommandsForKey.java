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

import java.util.Objects;

import accord.api.RoutingKey;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.NotifySink;
import accord.local.cfk.SafeCommandsForKey;
import accord.utils.Invariants;

import org.apache.cassandra.service.accord.AccordCacheEntry.LockMode;

public class AccordSafeCommandsForKey extends SafeCommandsForKey implements AccordSafeState<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>
{
    public static class CommandsForKeyCacheEntry extends AccordCacheEntry<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>
    {
        private NotifySink overrideSink;

        CommandsForKeyCacheEntry(RoutingKey key, AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance owner)
        {
            super(key, owner);
        }
    }

    private final AccordCacheEntry<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> global;

    public AccordSafeCommandsForKey(AccordCacheEntry<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> global)
    {
        super(global.key());
        this.global = global;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordSafeCommandsForKey that = (AccordSafeCommandsForKey) o;
        return Objects.equals(current, that.current);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "AccordSafeCommandsForKey{" +
               "state=" + statusString() +
               ", global=" + global +
               ", current=" + current +
               '}';
    }

    public final AccordCacheEntry<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> global()
    {
        return global;
    }

    @Override
    public void postExecute(AccordTask<?> owner)
    {
        global.releaseExclusive(this, owner);
    }

    @Override
    public void overrideSink(NotifySink overrideSink)
    {
        ((CommandsForKeyCacheEntry)global).overrideSink = overrideSink;
    }

    @Override
    public NotifySink overrideSink()
    {
        return ((CommandsForKeyCacheEntry)global).overrideSink;
    }

    public void preExecute(AccordTask<?> owner, LockMode lockMode)
    {
        requireUninitialised();
        current = global.lockExclusive(owner, lockMode);
        if (current == null)
            initialize();
        setSafe();
    }

}
