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

package org.apache.cassandra.harry.runner;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.visitors.Visitor;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Runner that allows to run for a specific number of logical timestamps, rather
 * than being configured by time.
 */
public class UpToLtsRunner extends Runner.SequentialRunner
{
    public static final String TYPE = "up_to_lts";

    private final long maxLts;

    public static Configuration.RunnerConfiguration factory(List<? extends Visitor.VisitorFactory> visitorFactories,
                                                            long maxLts,
                                                            long runtime, TimeUnit runtimeUnit)
    {
        return (r, c) -> new UpToLtsRunner(r, c, visitorFactories, maxLts, runtime, runtimeUnit);
    }

    public static Configuration.RunnerConfiguration factory(Visitor.VisitorFactory visitorFactory,
                                                            long maxLts,
                                                            long runtime, TimeUnit runtimeUnit)
    {
        return (r, c) -> new UpToLtsRunner(r, c, Collections.singletonList(visitorFactory), maxLts, runtime, runtimeUnit);
    }

    public UpToLtsRunner(Run run,
                         Configuration config,
                         List<? extends Visitor.VisitorFactory> visitorFactories,
                         long maxLts,
                         long runtime, TimeUnit runtimeUnit)
    {
        super(run, config, visitorFactories, runtime, runtimeUnit);
        this.maxLts = maxLts;
    }

    @Override
    public String type()
    {
        return TYPE;
    }

    @Override
    public void runInternal()
    {
        long deadline = nanoTime() + runtimeUnit.toNanos(runtime);
        while (run.tracker.maxStarted() < maxLts && nanoTime() < deadline)
        {
            for (Visitor visitor : visitors)
                visitor.visit();
        }
    }

    @JsonTypeName(TYPE)
    public static class UpToLtsRunnerConfig implements Configuration.RunnerConfiguration
    {
        public final List<Configuration.VisitorConfiguration> visitor_factories;
        public final long max_lts;
        public final long run_time;
        public final TimeUnit run_time_unit;

        @JsonCreator
        public UpToLtsRunnerConfig(@JsonProperty(value = "visitors") List<Configuration.VisitorConfiguration> visitors,
                                   @JsonProperty(value = "max_lts") long maxLts,
                                   @JsonProperty(value = "run_time", defaultValue = "2") long runtime,
                                   @JsonProperty(value = "run_time_unit", defaultValue = "HOURS") TimeUnit runtimeUnit)
        {
            this.visitor_factories = visitors;
            this.max_lts = maxLts;
            this.run_time = runtime;
            this.run_time_unit = runtimeUnit;
        }

        @Override
        public Runner make(Run run, Configuration config)
        {
            return new UpToLtsRunner(run, config, visitor_factories, max_lts, run_time, run_time_unit);
        }
    }
}
