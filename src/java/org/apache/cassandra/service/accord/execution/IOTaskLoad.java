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

package org.apache.cassandra.service.accord.execution;

import accord.utils.Invariants;

import static org.apache.cassandra.service.accord.execution.IOTaskLoad.FailureHolder.NOT_STARTED;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.LOAD;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.RANGE_LOAD;

public class IOTaskLoad<K, V> extends IOTask
{
    static class FailureHolder
    {
        static final FailureHolder NOT_STARTED = new FailureHolder(new RuntimeException("Not started"));

        final Throwable fail;

        FailureHolder(Throwable fail)
        {
            this.fail = fail;
        }
    }

    final AccordCacheEntry<K, V, ?> entry;
    Object result = NOT_STARTED;

    IOTaskLoad(AccordExecutor executor, AccordCacheEntry<K, V, ?> entry, GlobalGroup group)
    {
        super(executor, group);
        Invariants.require(group == LOAD || group == RANGE_LOAD);
        this.entry = entry;
    }

    @Override
    void maybeCompleteExclusiveMayThrow()
    {
        if (!(result instanceof FailureHolder))
            executor.onLoadedExclusive(entry, (V) result, null);
        else
            executor.onLoadedExclusive(entry, null, ((FailureHolder) result).fail);
        super.maybeCompleteExclusiveMayThrow();
    }

    @Override
    public boolean runMayThrow()
    {
        result = entry.owner.parent().adapter().load(entry.owner.commandStore, entry.key());
        return true;
    }

    @Override
    void reportFailureMayThrow(Throwable t)
    {
        result = new FailureHolder(t);
    }

    @Override
    public String description()
    {
        return "Load " + entry.key();
    }

    @Override
    public String briefDescription()
    {
        return description();
    }
}
