/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.stress.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.tools.NodeProbe;

public class JmxCollector implements Callable<JmxCollector.GcStats>
{

    public static class GcStats
    {
        public final double count;
        public final double bytes;
        public final double maxms;
        public final double summs;
        public final double sumsqms;
        public final double sdvms;
        public GcStats(double count, double bytes, double maxms, double summs, double sumsqms)
        {
            this.count = count;
            this.bytes = bytes;
            this.maxms = maxms;
            this.summs = summs;
            this.sumsqms = sumsqms;
            double mean = summs / count;
            double stdev = Math.sqrt((sumsqms / count) - (mean * mean));
            if (Double.isNaN(stdev))
                stdev = 0;
            this.sdvms = stdev;
        }
        public GcStats(double fill)
        {
            this(fill, fill, fill, fill, fill);
        }
        public static GcStats aggregate(List<GcStats> stats)
        {
            double count = 0, bytes = 0, maxms = 0, summs = 0, sumsqms = 0;
            for (GcStats stat : stats)
            {
                count += stat.count;
                bytes += stat.bytes;
                maxms += stat.maxms;
                summs += stat.summs;
                sumsqms += stat.sumsqms;
            }
            return new GcStats(count, bytes, maxms, summs, sumsqms);
        }
    }

    final NodeProbe[] probes;

    // TODO: should expand to whole cluster
    public JmxCollector(Collection<String> hosts, int port)
    {
        probes = new NodeProbe[hosts.size()];
        int i = 0;
        for (String host : hosts)
        {
            probes[i] = connect(host, port);
            probes[i].getAndResetGCStats();
            i++;
        }
    }

    private static NodeProbe connect(String host, int port)
    {
        try
        {
            return new NodeProbe(host, port);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public GcStats call() throws Exception
    {
        final List<Future<GcStats>> futures = new ArrayList<>();
        for (final NodeProbe probe : probes)
        {
            futures.add(TPE.submit(new Callable<GcStats>()
            {
                public GcStats call() throws Exception
                {
                    final double[] stats = probe.getAndResetGCStats();
                    return new GcStats(stats[5], stats[4], stats[1], stats[2], stats[3]);
                }
            }));
        }

        List<GcStats> results = new ArrayList<>();
        for (Future<GcStats> future : futures)
            results.add(future.get());
        return GcStats.aggregate(results);
    }

    private static final ExecutorService TPE = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), new NamedThreadFactory("JmxCollector"));
}
