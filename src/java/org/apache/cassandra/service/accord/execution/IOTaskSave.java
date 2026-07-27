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

import org.apache.cassandra.utils.Closeable;

import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.SAVE;

class IOTaskSave extends IOTask
{
    private static final Throwable NOT_STARTED = new Throwable();

    final AccordCacheEntry<?, ?, ?> entry;
    final AccordCacheEntry.UniqueSave identity;
    final Runnable run;
    Throwable failure = NOT_STARTED;

    IOTaskSave(AccordExecutor executor, AccordCacheEntry<?, ?, ?> entry, AccordCacheEntry.UniqueSave identity, Runnable run)
    {
        super(executor, SAVE);
        this.entry = entry;
        this.identity = identity;
        this.run = run;
    }

    @Override
    void postRunExclusive()
    {
        executor.onSavedExclusive(entry, identity, failure);
    }

    @Override
    String toDescription()
    {
        return "Save " + entry.key();
    }

    @Override
    public void run()
    {
        onRunning();
        try (Closeable close = resources.get())
        {
            run.run();
        }
        onRunComplete();
        failure = null;
    }

    @Override
    protected void reportFailure(Throwable t)
    {
        failure = t;
    }

    @Override
    public String description()
    {
        return "Save " + entry;
    }
}
