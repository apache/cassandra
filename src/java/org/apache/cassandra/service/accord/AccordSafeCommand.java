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

import com.google.common.annotations.VisibleForTesting;

import accord.local.Command;
import accord.local.Command.TransientListener;
import accord.local.Listeners;
import accord.local.SafeCommand;
import accord.primitives.TxnId;

public class AccordSafeCommand extends SafeCommand implements AccordSafeState<TxnId, Command>
{
    private boolean invalidated;
    private final AccordCachingState<TxnId, Command> global;
    private Command original;
    private Command current;

    public AccordSafeCommand(AccordCachingState<TxnId, Command> global)
    {
        super(global.key());
        this.global = global;
        this.original = null;
        this.current = null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordSafeCommand that = (AccordSafeCommand) o;
        return Objects.equals(original, that.original) && Objects.equals(current, that.current);
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
               "invalidated=" + invalidated +
               ", global=" + global +
               ", original=" + original +
               ", current=" + current +
               '}';
    }

    @Override
    public AccordCachingState<TxnId, Command> global()
    {
        checkNotInvalidated();
        return global;
    }

    @Override
    public Command current()
    {
        checkNotInvalidated();
        return current;
    }

    @Override
    @VisibleForTesting
    public void set(Command command)
    {
        checkNotInvalidated();
        this.current = command;
    }

    @Override
    public Command original()
    {
        checkNotInvalidated();
        return original;
    }

    public SavedCommand.SavedDiff diff()
    {
        return SavedCommand.diff(original, current);
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

    @Override
    public void addListener(TransientListener listener)
    {
        checkNotInvalidated();
        global.addListener(listener);
    }

    @Override
    public boolean removeListener(TransientListener listener)
    {
        checkNotInvalidated();
        return global.removeListener(listener);
    }

    @Override
    public Listeners<TransientListener> transientListeners()
    {
        checkNotInvalidated();
        return global.listeners();
    }
}
