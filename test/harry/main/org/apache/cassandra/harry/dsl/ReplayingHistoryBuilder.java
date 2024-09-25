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

package org.apache.cassandra.harry.dsl;

import java.util.function.Function;

import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.op.Visit;

public class ReplayingHistoryBuilder extends HistoryBuilder
{
    private final CQLVisitExecutor executor;

    public ReplayingHistoryBuilder(ValueGenerators generators, Function<HistoryBuilder, CQLVisitExecutor> executorFactory)
    {
        super((IndexedValueGenerators) generators);
        this.executor = executorFactory.apply(this);
     }

    SingleOperationVisitBuilder singleOpVisitBuilder()
    {
        long visitLts = nextOpIdx++;
        HistoryBuilder this_ = this;
        return new SingleOperationVisitBuilder(visitLts,
                                               valueGenerators,
                                               indexGenerators,
                                               (visit) -> log.put(visit.lts, visit)) {
            @Override
            Visit build()
            {
                Visit visit = super.build();
                CQLVisitExecutor.executeVisit(visit, executor, this_);
                return visit;
            }
        };
    }

    @Override
    public MultiOperationVisitBuilder multistep()
    {
        long visitLts = nextOpIdx++;
        HistoryBuilder this_ = this;
        return new MultiOperationVisitBuilder(visitLts,
                                              valueGenerators,
                                              indexGenerators,
                                              visit -> log.put(visit.lts, visit)) {
            @Override
            Visit buildInternal()
            {
                Visit visit = super.buildInternal();
                CQLVisitExecutor.executeVisit(visit, executor, this_);
                return visit;
            }
        };
    }
}