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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class StagedRunner extends Runner.TimedRunner
{
    public static final String TYPE = "staged";

    private final List<Runner> stages;

    public StagedRunner(Run run,
                        Configuration config,
                        List<Configuration.RunnerConfiguration> runnerFactories,
                        long runtime, TimeUnit runtimeUnit)
    {
        super(run, config, runtime, runtimeUnit);
        this.stages = new ArrayList<>();
        for (Configuration.RunnerConfiguration runner : runnerFactories)
            stages.add(runner.make(run, config));
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    protected void runInternal() throws Throwable
    {
        long deadline = nanoTime() + runtimeUnit.toNanos(nanoTime());

        while (nanoTime() < deadline)
        {
            for (Runner runner : stages)
                runner.runInternal();
        }
    }

    @JsonTypeName(TYPE)
    public static class StagedRunnerConfig implements Configuration.RunnerConfiguration
    {
        @JsonProperty(value = "stages")
        public final List<Configuration.RunnerConfiguration> runnerFactories;

        public final long run_time;
        public final TimeUnit run_time_unit;

        @JsonCreator
        public StagedRunnerConfig(@JsonProperty(value = "stages") List<Configuration.RunnerConfiguration> stages,
                                  @JsonProperty(value = "run_time", defaultValue = "2") long runtime,
                                  @JsonProperty(value = "run_time_unit", defaultValue = "HOURS") TimeUnit runtimeUnit)
        {
            this.runnerFactories = stages;
            this.run_time = runtime;
            this.run_time_unit = runtimeUnit;
        }

        @Override
        public Runner make(Run run, Configuration config)
        {
            return new StagedRunner(run, config, runnerFactories, run_time, run_time_unit);
        }
    }
}
