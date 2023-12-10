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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.operations.QueryGenerator;

public class RandomPartitionValidator implements Visitor
{
    protected final Model model;
    protected final QueryGenerator.TypedQueryGenerator queryGenerator;
    protected final Run run;
    protected final AtomicLong modifier;
    protected final QueryLogger logger;

    public RandomPartitionValidator(Run run,
                                    Model.ModelFactory modelFactory,
                                    QueryLogger logger)
    {
        this.model = modelFactory.make(run);
        this.queryGenerator = new QueryGenerator.TypedQueryGenerator(run);
        this.run = run;
        this.modifier = new AtomicLong();
        this.logger = logger;
    }

    public void visit()
    {
        if (run.tracker.maxStarted() == 0)
            return;

        long modifier = this.modifier.incrementAndGet();
        long pd = ((OpSelectors.DefaultPdSelector)run.pdSelector).randomVisitedPd(run.tracker.maxStarted(),
                                                                                  modifier,
                                                                                  run.schemaSpec);
        model.validate(queryGenerator.inflate(run.pdSelector.maxLtsFor(pd), modifier));
    }
}
