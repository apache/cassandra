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

import accord.api.Journal;
import accord.local.Command;
import accord.local.SafeCommand;
import accord.primitives.TxnId;

import org.apache.cassandra.service.accord.AccordCacheEntry.LockMode;

public class AccordSafeCommand extends SafeCommand implements AccordSafeState<TxnId, Command, AccordSafeCommand>
{
    private final AccordCacheEntry<TxnId, Command, AccordSafeCommand> global;
    private Command original;

    public AccordSafeCommand(AccordCacheEntry<TxnId, Command, AccordSafeCommand> global)
    {
        super(global.key());
        this.global = global;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordSafeCommand that = (AccordSafeCommand) o;
        return Objects.equals(this.original, that.original) && Objects.equals(this.current(), that.current());
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "AccordSafeCommand{" +
               "status=" + statusString() +
               ", global=" + global +
               ", original=" + original +
               ", current=" + current +
               '}';
    }

    public AccordCacheEntry<TxnId, Command, AccordSafeCommand> global()
    {
        return global;
    }

    @Override
    public void postExecute(AccordTask<?> owner)
    {
        global.releaseExclusive(this, owner);
    }

    public Journal.CommandUpdate update()
    {
        return new Journal.CommandUpdate(original, current());
    }

    public void preExecute(AccordTask<?> owner, LockMode lockMode)
    {
        requireUninitialised();
        original = global.lockExclusive(owner, lockMode);
        current = original;
        if (current == null)
            initialise();
        setSafe();
    }
}
