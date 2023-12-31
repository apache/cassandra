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

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.rng.RngUtils;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.model.SelectHelper;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.tracker.DataTracker;

public class Sampler implements Visitor
{
    private static final Logger logger = LoggerFactory.getLogger(AllPartitionsValidator.class);

    private final SystemUnderTest sut;
    private final OpSelectors.PdSelector pdSelector;
    private final OpSelectors.Clock clock;
    private final DataTracker tracker;
    private final SchemaSpec schema;
    private final int samplePartitions;

    public Sampler(Run run, int samplePartitions)
    {
        this.sut = run.sut;
        this.pdSelector = run.pdSelector;
        this.clock = run.clock;
        this.tracker = run.tracker;
        this.schema = run.schemaSpec;
        this.samplePartitions = samplePartitions;
    }

    public void visit()
    {
        long lts = clock.peek();
        long max = pdSelector.maxPosition(tracker.maxStarted());
        DescriptiveStatistics ds = new DescriptiveStatistics();
        int empty = 0;

        long n = RngUtils.next(lts);
        for (long i = 0; i < this.samplePartitions; i++)
        {
            long posLts = pdSelector.minLtsAt(RngUtils.asInt(n, (int) max));
            n = RngUtils.next(n);
            // TODO: why not just pd at pos?
            long pd = pdSelector.pd(posLts, schema);
            long count = (long) sut.execute(SelectHelper.count(schema, pd), SystemUnderTest.ConsistencyLevel.ONE)[0][0];
            if (count == 0)
                empty++;
            ds.addValue(count);
        }
        logger.info("Visited {} partitions (sampled {} empty out of {}), with mean size of {}. Median: {}. Min: {}. Max: {}",
                    max, empty, samplePartitions, ds.getMean(), ds.getPercentile(0.5), ds.getMin(), ds.getMax());
    }

    @JsonTypeName("sampler")
    public static class SamplerConfiguration implements Configuration.VisitorConfiguration
    {
        public final int sample_partitions;

        @JsonCreator
        public SamplerConfiguration(@JsonProperty(value = "sample_partitions", defaultValue = "10") int sample_partitions)
        {
            this.sample_partitions = sample_partitions;
        }

        public Visitor make(Run run)
        {
            return new Sampler(run, sample_partitions);
        }
    }
}
