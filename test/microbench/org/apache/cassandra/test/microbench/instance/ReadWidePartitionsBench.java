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

package org.apache.cassandra.test.microbench.instance;


import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;

public class ReadWidePartitionsBench extends ReadBenchBase
{
    @Param({"1000", "4"}) // wide and very wide partitions
    int partitions = 4;

    @Override
    public Object[] writeArguments(long i)
    {
        return new Object[] { i % partitions, i, i };
    }

    Object[] readArguments(long i, long offset)
    {
        return new Object[] { (i + offset) % partitions, i };
    }

    @Benchmark
    public Object readRandomInside() throws Throwable
    {
        return performRead("SELECT * from " + table + " where userid=? and picid=?",
                           () -> readArguments(rand.nextInt(count),0));
    }

    @Benchmark
    public Object readRandomWOutside() throws Throwable
    {
        return performRead("SELECT * from " + table + " where userid=? and picid=?",
                           () -> readArguments(rand.nextInt(count), rand.nextInt(6) == 1 ? 1 : 0));
    }

    @Benchmark
    public Object readFixed() throws Throwable
    {
        return performRead("SELECT * from " + table + " where userid=? and picid=?",
                           () -> readArguments(1234567890123L % count, 0));
    }

    @Benchmark
    public Object readOutside() throws Throwable
    {
        return performRead("SELECT * from " + table + " where userid=? and picid=?",
                           () -> readArguments(1234567890123L % count, 1));
    }

    @Benchmark
    public Object readGreaterMatch() throws Throwable
    {
        return performRead("SELECT * from "+table+" where userid=? and picid>? limit 1",
                           () -> readArguments(rand.nextInt(count), 0));
    }

    @Benchmark
    public Object readReversedMatch() throws Throwable
    {
        return performRead("SELECT * from "+table+" where userid=? and picid<? order by picid desc limit 1",
                           () -> readArguments(rand.nextInt(count), 0));
    }

    @Benchmark
    public Object readGreater() throws Throwable
    {
        return performRead("SELECT * from "+table+" where userid=? and picid>? limit 1",
                           () -> readArguments(rand.nextInt(count), 1));
    }

    @Benchmark
    public Object readReversed() throws Throwable
    {
        return performRead("SELECT * from "+table+" where userid=? and picid<? order by picid desc limit 1",
                           () -> readArguments(rand.nextInt(count), -1));
    }
}
