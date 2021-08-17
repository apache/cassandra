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

package org.apache.cassandra.test.microbench;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventPersistence;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.test.microbench.DiagnosticEventServiceBench.DummyEvent;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@Threads(4)
@State(Scope.Benchmark)
public class DiagnosticEventPersistenceBench
{
    private DiagnosticEventService service = DiagnosticEventService.instance();
    private DiagnosticEventPersistence persistence = DiagnosticEventPersistence.instance();
    private DiagnosticEvent event = new DummyEvent();

    @Setup
    public void setup()
    {
        OverrideConfigurationLoader.override((config) -> {
            config.diagnostic_events_enabled = true;
        });
        DatabaseDescriptor.daemonInitialization();

        service.cleanup();

        // make persistence subscribe to and store events
        persistence.enableEventPersistence(DummyEvent.class.getName());
    }

    @Benchmark
    public void persistEvents()
    {
        service.publish(event);
    }
}
