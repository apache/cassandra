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

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.corruptor.AddExtraRowCorruptor;
import org.apache.cassandra.harry.corruptor.ChangeValueCorruptor;
import org.apache.cassandra.harry.corruptor.HideRowCorruptor;
import org.apache.cassandra.harry.corruptor.HideValueCorruptor;
import org.apache.cassandra.harry.corruptor.QueryResponseCorruptor;
import org.apache.cassandra.harry.runner.HarryRunner;
import org.apache.cassandra.harry.operations.Query;

public class CorruptingVisitor implements Visitor
{
    public static final Logger logger = LoggerFactory.getLogger(HarryRunner.class);

    private final Run run;
    private final QueryResponseCorruptor[] corruptors;
    private final int triggerAfter;

    public CorruptingVisitor(int triggerAfter,
                             Run run)
    {
        this.run = run;
        this.triggerAfter = triggerAfter;

        this.corruptors = new QueryResponseCorruptor[]{
        new QueryResponseCorruptor.SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                run.clock,
                                                                HideRowCorruptor::new),
        new AddExtraRowCorruptor(run.schemaSpec,
                                 run.clock,
                                 run.tracker,
                                 run.descriptorSelector),
        new QueryResponseCorruptor.SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                run.clock,
                                                                HideValueCorruptor::new),
        new QueryResponseCorruptor.SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                run.clock,
                                                                ChangeValueCorruptor::new)
        };
    }

    public void visit()
    {
        long lts = run.clock.peek();

        if (lts > triggerAfter)
            return;

        // TODO: switch to a better entropy source
        Random random = new Random(1);

        QueryResponseCorruptor corruptor = corruptors[random.nextInt(corruptors.length)];
        long maxPos = run.pdSelector.maxPosition(run.tracker.maxStarted());
        long pd = run.pdSelector.pd(random.nextInt((int) maxPos), run.schemaSpec);
        try
        {
            boolean success = corruptor.maybeCorrupt(Query.selectAllColumns(run.schemaSpec, pd, false),
                                                     run.sut);
            logger.info("{} tried to corrupt a partition with a pd {}@{} my means of {}", success ? "Successfully" : "Unsuccessfully", pd, lts, corruptor.getClass());
        }
        catch (Throwable t)
        {
            logger.error("Caught an exception while trying to corrupt a partition.", t);
        }
    }
}
