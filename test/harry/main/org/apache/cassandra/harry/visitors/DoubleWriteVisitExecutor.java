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

package org.apache.cassandra.harry.visitors;

public class DoubleWriteVisitExecutor extends VisitExecutor
{
    private final VisitExecutor delegate1;
    private final VisitExecutor delegate2;

    public DoubleWriteVisitExecutor(VisitExecutor delegate1,
                                    VisitExecutor delegate2)
    {
        this.delegate1 = delegate1;
        this.delegate2 = delegate2;
    }

    protected void beforeLts(long lts, long pd)
    {
        delegate1.beforeLts(lts, pd);
        delegate2.beforeLts(lts, pd);
    }

    protected void afterLts(long lts, long pd)
    {
        delegate1.afterLts(lts, pd);
        delegate2.afterLts(lts, pd);
    }

    protected void operation(Operation operation)
    {
        delegate1.operation(operation);
        delegate2.operation(operation);
    }

    public void shutdown() throws InterruptedException
    {
        delegate1.shutdown();
        delegate2.shutdown();
    }
}
