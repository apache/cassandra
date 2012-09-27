/**
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

package org.apache.cassandra.service;

import java.io.Serializable;

public class PBSPredictionResult implements Serializable
{
    private int n;
    private int r;
    private int w;

    private float timeSinceWrite;
    private int numberVersionsStale;

    private float consistencyProbability;

    private float averageReadLatency;
    private float averageWriteLatency;
    private long percentileReadLatencyValue;
    private float percentileReadLatencyPercentile;
    private long percentileWriteLatencyValue;
    private float percentileWriteLatencyPercentile;

    public PBSPredictionResult(int n,
                               int r,
                               int w,
                               float timeSinceWrite,
                               int numberVersionsStale,
                               float consistencyProbability,
                               float averageReadLatency,
                               float averageWriteLatency,
                               long percentileReadLatencyValue,
                               float percentileReadLatencyPercentile,
                               long percentileWriteLatencyValue,
                               float percentileWriteLatencyPercentile) {
        this.n = n;
        this.r = r;
        this.w = w;
        this.timeSinceWrite = timeSinceWrite;
        this.numberVersionsStale = numberVersionsStale;
        this.consistencyProbability = consistencyProbability;
        this.averageReadLatency = averageReadLatency;
        this.averageWriteLatency = averageWriteLatency;
        this.percentileReadLatencyValue = percentileReadLatencyValue;
        this.percentileReadLatencyPercentile = percentileReadLatencyPercentile;
        this.percentileWriteLatencyValue = percentileWriteLatencyValue;
        this.percentileWriteLatencyPercentile = percentileWriteLatencyPercentile;
    }

    public int getN()
    {
        return n;
    }

    public int getR()
    {
        return r;
    }

    public int getW()
    {
        return w;
    }

    public float getTimeSinceWrite()
    {
        return timeSinceWrite;
    }

    public int getNumberVersionsStale()
    {
        return numberVersionsStale;
    }

    public float getConsistencyProbability()
    {
        return consistencyProbability;
    }

    public float getAverageReadLatency()
    {
        return averageReadLatency;
    }

    public float getAverageWriteLatency()
    {
        return averageWriteLatency;
    }

    public long getPercentileReadLatencyValue()
    {
        return percentileReadLatencyValue;
    }

    public float getPercentileReadLatencyPercentile()
    {
        return percentileReadLatencyPercentile;
    }

    public long getPercentileWriteLatencyValue()
    {
        return percentileWriteLatencyValue;
    }

    public float getPercentileWriteLatencyPercentile()
    {
        return percentileWriteLatencyPercentile;
    }
}

