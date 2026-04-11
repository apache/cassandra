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

package org.apache.cassandra.db.compaction;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Collectors;

import org.apache.cassandra.utils.FBUtilities;

public class CompactionTasks extends AbstractCollection<AbstractCompactionTask> implements AutoCloseable
{
    private static final CompactionTasks EMPTY = new CompactionTasks(Collections.emptyList());

    private final Collection<AbstractCompactionTask> tasks;

    private CompactionTasks(Collection<AbstractCompactionTask> tasks)
    {
        this.tasks = tasks;
    }

    public static CompactionTasks create(Collection<AbstractCompactionTask> tasks)
    {
        if (tasks == null || tasks.isEmpty())
            return EMPTY;
        return new CompactionTasks(tasks);
    }

    public static CompactionTasks empty()
    {
        return EMPTY;
    }

    public Iterator<AbstractCompactionTask> iterator()
    {
        return tasks.iterator();
    }

    public int size()
    {
        return tasks.size();
    }

    public void close()
    {
        try
        {
            FBUtilities.closeAll(tasks.stream().map(task -> task.transaction).collect(Collectors.toList()));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
