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

import java.util.Arrays;
import java.util.function.LongSupplier;

public abstract class ReplayingVisitor extends LtsVisitor
{
    public ReplayingVisitor(VisitExecutor delegate, LongSupplier ltsSource)
    {
        super(delegate, ltsSource);
    }

    @Override
    public void visit(long lts)
    {
        replay(getVisit(lts));
    }

    public abstract Visit getVisit(long lts);

    public abstract void replayAll();

    private void replay(Visit visit)
    {
        beforeLts(visit.lts, visit.pd);
        for (Operation operation : visit.operations)
            operation(operation);
        afterLts(visit.lts, visit.pd);
    }

    public static class Visit
    {
        public final long lts;
        public final long pd;
        public final Operation[] operations;

        public Visit(long lts, long pd, Operation[] operations)
        {
            this.lts = lts;
            this.pd = pd;
            this.operations = operations;
        }

        public String toString()
        {
            return "Visit{" +
                   "lts=" + lts +
                   ", pd=" + pd +
                   ", operations=[" + Arrays.toString(operations) +
                                            "]}";
        }


    }
}