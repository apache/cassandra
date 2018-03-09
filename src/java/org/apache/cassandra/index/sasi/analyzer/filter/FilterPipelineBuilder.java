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
package org.apache.cassandra.index.sasi.analyzer.filter;

/**
 * Creates a Pipeline object for applying n pieces of logic
 * from the provided methods to the builder in a guaranteed order
 */
public class FilterPipelineBuilder
{
    private final FilterPipelineTask<?,?> parent;
    private FilterPipelineTask<?,?> current;

    public FilterPipelineBuilder(FilterPipelineTask<?, ?> first)
    {
        this(first, first);
    }

    private FilterPipelineBuilder(FilterPipelineTask<?, ?> first, FilterPipelineTask<?, ?> current)
    {
        this.parent = first;
        this.current = current;
    }

    public FilterPipelineBuilder add(String name, FilterPipelineTask<?,?> nextTask)
    {
        this.current.setLast(name, nextTask);
        this.current = nextTask;
        return this;
    }

    public FilterPipelineTask<?,?> build()
    {
        return this.parent;
    }
}
