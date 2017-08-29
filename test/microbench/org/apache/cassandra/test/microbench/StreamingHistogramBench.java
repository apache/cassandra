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


import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.Random;

import org.apache.cassandra.utils.StreamingHistogram;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class StreamingHistogramBench
{
    StreamingHistogram.StreamingHistogramBuilder streamingHistogram;
    StreamingHistogram.StreamingHistogramBuilder newStreamingHistogram;
    StreamingHistogram.StreamingHistogramBuilder newStreamingHistogram2;
    StreamingHistogram.StreamingHistogramBuilder newStreamingHistogram3;
    StreamingHistogram.StreamingHistogramBuilder newStreamingHistogram4;
    StreamingHistogram.StreamingHistogramBuilder newStreamingHistogram5;
    StreamingHistogram.StreamingHistogramBuilder newStreamingHistogram6;
    StreamingHistogram.StreamingHistogramBuilder streamingHistogram60;
    StreamingHistogram.StreamingHistogramBuilder newStreamingHistogram60;
    StreamingHistogram.StreamingHistogramBuilder newStreamingHistogram100x60;

    StreamingHistogram.StreamingHistogramBuilder narrowstreamingHistogram;
    StreamingHistogram.StreamingHistogramBuilder narrownewStreamingHistogram;
    StreamingHistogram.StreamingHistogramBuilder narrownewStreamingHistogram2;
    StreamingHistogram.StreamingHistogramBuilder narrownewStreamingHistogram3;
    StreamingHistogram.StreamingHistogramBuilder narrownewStreamingHistogram4;
    StreamingHistogram.StreamingHistogramBuilder narrownewStreamingHistogram5;
    StreamingHistogram.StreamingHistogramBuilder narrownewStreamingHistogram6;
    StreamingHistogram.StreamingHistogramBuilder narrownewStreamingHistogram60;
    StreamingHistogram.StreamingHistogramBuilder narrowstreamingHistogram60;
    StreamingHistogram.StreamingHistogramBuilder narrownewStreamingHistogram100x60;

    StreamingHistogram.StreamingHistogramBuilder sparsestreamingHistogram;
    StreamingHistogram.StreamingHistogramBuilder sparsenewStreamingHistogram;
    StreamingHistogram.StreamingHistogramBuilder sparsenewStreamingHistogram2;
    StreamingHistogram.StreamingHistogramBuilder sparsenewStreamingHistogram3;
    StreamingHistogram.StreamingHistogramBuilder sparsenewStreamingHistogram4;
    StreamingHistogram.StreamingHistogramBuilder sparsenewStreamingHistogram5;
    StreamingHistogram.StreamingHistogramBuilder sparsenewStreamingHistogram6;
    StreamingHistogram.StreamingHistogramBuilder sparsestreamingHistogram60;
    StreamingHistogram.StreamingHistogramBuilder sparsenewStreamingHistogram60;
    StreamingHistogram.StreamingHistogramBuilder sparsenewStreamingHistogram100x60;

    static int[] ttls = new int[10000000];
    static int[] narrowttls = new int[10000000];
    static int[] sparsettls = new int[10000000];
    static
    {
        Random random = new Random();
        for(int i = 0 ; i < 10000000; i++)
        {
            // Seconds in a day
            ttls[i] = random.nextInt(86400);
            // Seconds in 3 hours
            narrowttls[i] = random.nextInt(14400);
            // Seconds in a minute
            sparsettls[i] = random.nextInt(60);
        }
    }

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {

        streamingHistogram = new StreamingHistogram.StreamingHistogramBuilder(100, 0, 1);
        newStreamingHistogram = new StreamingHistogram.StreamingHistogramBuilder(100, 1000, 1);
        newStreamingHistogram2 = new StreamingHistogram.StreamingHistogramBuilder(100, 10000, 1);
        newStreamingHistogram3 = new StreamingHistogram.StreamingHistogramBuilder(100, 100000, 1);
        newStreamingHistogram4 = new StreamingHistogram.StreamingHistogramBuilder(50, 100000, 1);
        newStreamingHistogram5 = new StreamingHistogram.StreamingHistogramBuilder(50, 10000,1 );
        newStreamingHistogram6 = new StreamingHistogram.StreamingHistogramBuilder(100, 1000000, 1);
        streamingHistogram60 = new StreamingHistogram.StreamingHistogramBuilder(100, 0, 60);
        newStreamingHistogram60 = new StreamingHistogram.StreamingHistogramBuilder(100, 100000, 60);
        newStreamingHistogram100x60 = new StreamingHistogram.StreamingHistogramBuilder(100, 10000, 60);

        narrowstreamingHistogram = new StreamingHistogram.StreamingHistogramBuilder(100, 0, 1);
        narrownewStreamingHistogram = new StreamingHistogram.StreamingHistogramBuilder(100, 1000, 1);
        narrownewStreamingHistogram2 = new StreamingHistogram.StreamingHistogramBuilder(100, 10000, 1);
        narrownewStreamingHistogram3 = new StreamingHistogram.StreamingHistogramBuilder(100, 100000, 1);
        narrownewStreamingHistogram4 = new StreamingHistogram.StreamingHistogramBuilder(50, 100000, 1);
        narrownewStreamingHistogram5 = new StreamingHistogram.StreamingHistogramBuilder(50, 10000, 1);
        narrownewStreamingHistogram6 = new StreamingHistogram.StreamingHistogramBuilder(100, 1000000, 1);
        narrowstreamingHistogram60 = new StreamingHistogram.StreamingHistogramBuilder(100, 0, 60);
        narrownewStreamingHistogram60 = new StreamingHistogram.StreamingHistogramBuilder(100, 100000, 60);
        narrownewStreamingHistogram100x60 = new StreamingHistogram.StreamingHistogramBuilder(100, 10000, 60);


        sparsestreamingHistogram = new StreamingHistogram.StreamingHistogramBuilder(100, 0, 1);
        sparsenewStreamingHistogram = new StreamingHistogram.StreamingHistogramBuilder(100, 1000, 1);
        sparsenewStreamingHistogram2 = new StreamingHistogram.StreamingHistogramBuilder(100, 10000, 1);
        sparsenewStreamingHistogram3 = new StreamingHistogram.StreamingHistogramBuilder(100, 100000, 1);
        sparsenewStreamingHistogram4 = new StreamingHistogram.StreamingHistogramBuilder(50, 100000, 1);
        sparsenewStreamingHistogram5 = new StreamingHistogram.StreamingHistogramBuilder(50, 10000, 1);
        sparsenewStreamingHistogram6 = new StreamingHistogram.StreamingHistogramBuilder(100, 1000000, 1);
        sparsestreamingHistogram60 = new StreamingHistogram.StreamingHistogramBuilder(100, 0, 60);
        sparsenewStreamingHistogram60 = new StreamingHistogram.StreamingHistogramBuilder(100, 100000, 60);
        sparsenewStreamingHistogram100x60 = new StreamingHistogram.StreamingHistogramBuilder(100, 10000, 60);

    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {

    }

    @Benchmark
    public void existingSH() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            streamingHistogram.update(ttls[i]);
        streamingHistogram.build();
    }

    @Benchmark
    public void newSH10x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            newStreamingHistogram.update(ttls[i]);
        newStreamingHistogram.build();

    }

    @Benchmark
    public void newSH100x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            newStreamingHistogram2.update(ttls[i]);
        newStreamingHistogram2.build();

    }

    @Benchmark
    public void newSH1000x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            newStreamingHistogram3.update(ttls[i]);
        newStreamingHistogram3.build();

    }

    @Benchmark
    public void newSH10000x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            newStreamingHistogram6.update(ttls[i]);
        newStreamingHistogram6.build();

    }


    @Benchmark
    public void newSH50and1000() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            newStreamingHistogram4.update(ttls[i]);
        newStreamingHistogram4.build();

    }

    @Benchmark
    public void newSH50and100x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            newStreamingHistogram5.update(ttls[i]);
        newStreamingHistogram5.build();

    }

    @Benchmark
    public void streaminghistogram60s() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            streamingHistogram60.update(sparsettls[i]);
        streamingHistogram60.build();

    }

    @Benchmark
    public void newstreaminghistogram1000x60s() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            newStreamingHistogram60.update(sparsettls[i]);
        newStreamingHistogram60.build();
    }

    @Benchmark
    public void newstreaminghistogram100x60s() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            newStreamingHistogram100x60.update(sparsettls[i]);
        newStreamingHistogram100x60.build();
    }


    @Benchmark
    public void narrowexistingSH() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            narrowstreamingHistogram.update(narrowttls[i]);
        narrowstreamingHistogram.build();
    }

    @Benchmark
    public void narrownewSH10x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            narrownewStreamingHistogram.update(narrowttls[i]);
        narrownewStreamingHistogram.build();

    }

    @Benchmark
    public void narrownewSH100x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            narrownewStreamingHistogram2.update(narrowttls[i]);
        narrownewStreamingHistogram2.build();

    }

    @Benchmark
    public void narrownewSH1000x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            narrownewStreamingHistogram3.update(narrowttls[i]);
        narrownewStreamingHistogram3.build();

    }

    @Benchmark
    public void narrownewSH10000x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            narrownewStreamingHistogram6.update(ttls[i]);
        narrownewStreamingHistogram6.build();

    }


    @Benchmark
    public void narrownewSH50and1000x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            narrownewStreamingHistogram4.update(narrowttls[i]);
        narrownewStreamingHistogram4.build();

    }

    @Benchmark
    public void narrownewSH50and100x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            narrownewStreamingHistogram5.update(narrowttls[i]);
        narrownewStreamingHistogram5.build();

    }

    @Benchmark
    public void narrowstreaminghistogram60s() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            narrowstreamingHistogram60.update(sparsettls[i]);
        narrowstreamingHistogram60.build();

    }

    @Benchmark
    public void narrownewstreaminghistogram1000x60s() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            narrownewStreamingHistogram60.update(sparsettls[i]);
        narrownewStreamingHistogram60.build();

    }

    @Benchmark
    public void narrownewstreaminghistogram100x60s() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            narrownewStreamingHistogram100x60.update(sparsettls[i]);
        narrownewStreamingHistogram100x60.build();

    }


    @Benchmark
    public void sparseexistingSH() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            sparsestreamingHistogram.update(sparsettls[i]);
        sparsestreamingHistogram.build();
    }

    @Benchmark
    public void sparsenewSH10x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            sparsenewStreamingHistogram.update(sparsettls[i]);
        sparsenewStreamingHistogram.build();

    }

    @Benchmark
    public void sparsenewSH100x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            sparsenewStreamingHistogram2.update(sparsettls[i]);
        sparsenewStreamingHistogram2.build();

    }

    @Benchmark
    public void sparsenewSH1000x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            sparsenewStreamingHistogram3.update(sparsettls[i]);
        sparsenewStreamingHistogram3.build();

    }

    @Benchmark
    public void sparsenewSH10000x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            sparsenewStreamingHistogram6.update(ttls[i]);
        sparsenewStreamingHistogram6.build();
    }


    @Benchmark
    public void sparsenewSH50and1000x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            sparsenewStreamingHistogram4.update(sparsettls[i]);
        sparsenewStreamingHistogram4.build();

    }

    @Benchmark
    public void sparsenewSH50and100x() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            sparsenewStreamingHistogram5.update(sparsettls[i]);
        sparsenewStreamingHistogram5.build();

    }

    @Benchmark
    public void sparsestreaminghistogram60s() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            sparsestreamingHistogram60.update(sparsettls[i]);
        sparsestreamingHistogram60.build();

    }

    @Benchmark
    public void sparsenewstreaminghistogram1000x60() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            sparsenewStreamingHistogram60.update(sparsettls[i]);
        sparsenewStreamingHistogram60.build();

    }

    @Benchmark
    public void sparsenewstreaminghistogram100x60() throws Throwable
    {
        for(int i = 0 ; i < ttls.length; i++)
            sparsenewStreamingHistogram100x60.update(sparsettls[i]);
        sparsenewStreamingHistogram100x60.build();

    }
}
