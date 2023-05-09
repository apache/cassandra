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

package org.apache.cassandra.index.sai.analyzer.filter;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

/**
 * A linked list of {@link Task} objects. Used to apply a sequence of filtering tasks
 * to provided textual input in a guaranteed order.
 */
@NotThreadSafe
public class FilterPipeline
{
    private final Task head;
    private Task tail;

    public FilterPipeline(Task first)
    {
        this(first, first);
    }

    private FilterPipeline(Task first, Task tail)
    {
        this.head = first;
        this.tail = tail;
    }

    public FilterPipeline add(String name, Task task)
    {
        Preconditions.checkArgument(task != this.tail, "Provided last task [" + task.name + "] cannot be set to itself");
        
        this.tail.next = task;
        this.tail.name = name;
        
        this.tail = task;
        return this;
    }

    public Task head()
    {
        return this.head;
    }

    public abstract static class Task
    {
        public String name;
        public Task next;

        public abstract String process(String input);
    }
}
