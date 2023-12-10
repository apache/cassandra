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

import org.apache.cassandra.harry.core.MetricReporter;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.gen.Surjections;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.operations.QueryGenerator;

public class RecentValidator implements Visitor
{
    private final QueryLogger queryLogger;
    private final Model model;

    private final OpSelectors.PdSelector pdSelector;
    private final QueryGenerator.TypedQueryGenerator querySelector;
    private final MetricReporter metricReporter;
    private final OpSelectors.Clock clock;

    private final int partitionCount;
    private final int queries;

    public RecentValidator(int partitionCount,
                           int queries,
                           Run run,
                           Model.ModelFactory modelFactory,
                           QueryLogger queryLogger)
    {
        this.partitionCount = partitionCount;
        this.queries = Math.max(queries, 1);
        this.metricReporter = run.metricReporter;
        this.pdSelector = run.pdSelector;
        this.clock = run.clock;
        this.querySelector = new QueryGenerator.TypedQueryGenerator(run.rng,
                                                                    // TODO: make query kind configurable
                                                                    Surjections.enumValues(Query.QueryKind.class),
                                                                    run.rangeSelector);
        this.model = modelFactory.make(run);
        this.queryLogger = queryLogger;
    }

    // TODO: expose metric, how many times validated recent partitions
    private int validateRecentPartitions()
    {
        long pos = pdSelector.maxPosition(clock.peek());

        int maxPartitions = partitionCount;
        while (pos >= 0 && maxPartitions > 0 && !Thread.currentThread().isInterrupted())
        {
            long visitLts = pdSelector.minLtsAt(pos);
            for (int i = 0; i < queries; i++)
            {
                metricReporter.validateRandomQuery();
                Query query = querySelector.inflate(visitLts, i);
                // TODO: add pd skipping from shrinker here, too
                log(i, query);
                model.validate(query);
            }

            pos--;
            maxPartitions--;
        }
        
        return partitionCount - maxPartitions;
    }

    @Override
    public void visit()
    {
        validateRecentPartitions();
    }

    private void log(int modifier, Query query)
    {
        queryLogger.println(String.format("PD: %d. Modifier: %d.\t%s", query.pd, modifier, query.toSelectStatement()));
    }
}