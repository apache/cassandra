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
 * A single task or set of work to process an input
 * and return a single output. Maintains a link to the
 * next task to be executed after itself
 */
public abstract class FilterPipelineTask<F, T>
{
    private String name;
    public FilterPipelineTask<?, ?> next;

    protected <K, V> void setLast(String name, FilterPipelineTask<K, V> last)
    {
        if (last == this)
            throw new IllegalArgumentException("provided last task [" + last.name + "] cannot be set to itself");

        if (this.next == null)
        {
            this.next = last;
            this.name = name;
        }
        else
        {
            this.next.setLast(name, last);
        }
    }

    public abstract T process(F input) throws Exception;

    public String getName()
    {
        return name;
    }
}
