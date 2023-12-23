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

import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common class for all visitors that support visits at a specific logical timestamp.
 *
 * Classes inheriting from LTS Visitor have to visit drawn LTS: every LTS that has been received
 * from the consumer _has to_ actually be visited. If this is not done, during model checking
 * drawn LTS will be considered visited, which will lead to data inconsistencies.
 *
 * This class and its implementations such as Mutating visitor are NOT thread safe. If you'd like
 * to have several threads generating data, use multiple copies of delegating visitor, since
 */
public abstract class LtsVisitor extends VisitExecutor implements Visitor
{
    private static final Logger logger = LoggerFactory.getLogger(LtsVisitor.class);
    protected final VisitExecutor delegate;
    private final LongSupplier ltsSource;

    public LtsVisitor(VisitExecutor delegate,
                      LongSupplier ltsSource)
    {
        this.delegate = delegate;
        this.ltsSource = ltsSource;
    }

    public final void visit()
    {
        long lts = ltsSource.getAsLong();
        if (lts > 0 && lts % 10_000 == 0)
            logger.info("Visiting lts {}...", lts);
        visit(lts);
    }

    public abstract void visit(long lts);

    @Override
    protected void beforeLts(long lts, long pd)
    {
        delegate.beforeLts(lts, pd);
    }

    @Override
    protected void afterLts(long lts, long pd)
    {
        delegate.afterLts(lts, pd);
    }

    @Override
    protected void operation(Operation operation)
    {
        delegate.operation(operation);
    }

    @Override
    public void shutdown() throws InterruptedException
    {
        delegate.shutdown();
    }

}
