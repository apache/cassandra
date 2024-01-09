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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.visitors.LtsVisitor;
import org.apache.cassandra.harry.visitors.SkippingVisitor;
import org.apache.cassandra.harry.visitors.Visitor;

/**
 * A most trivial imaginable shrinker: attempts to skip partitions and/or logical timestamps to see if the
 * issue is still reproducible.
 */
public class TrivialShrinker
{
    public static void main(String[] args) throws Throwable
    {
        try
        {
            File configFile = HarryRunner.loadConfig(args);
            Configuration configuration = Configuration.fromFile(configFile);
            System.out.println(Configuration.toYamlString(configuration));

            Set<Long> pdsToSkip = new HashSet<>(Arrays.asList(
            // put pds you want to skip here, or Harry will find them for you
            ));

            Set<Long> ltsToSkip = new HashSet<>(Arrays.asList(
            // put lts you want to skip here, or Harry will find them for you
            ));

            // Which LTS failure has occurred on
            final long maxLts = 7000L;

            // Check if we've found exactly the exception that is causing the failure
            Predicate<Exception> check = (e) -> true;

            Run run = configuration.createRun();
            Configuration.SequentialRunnerConfig config = (Configuration.SequentialRunnerConfig) configuration.runner;
            List<Visitor> visitors = new ArrayList<>();
            for (Configuration.VisitorConfiguration factory : config.visitorFactories)
            {
                Visitor visitor = factory.make(run);
                if (visitor instanceof LtsVisitor)
                {
                    AtomicLong counter = new AtomicLong();
                    visitors.add(new SkippingVisitor((LtsVisitor) visitor,
                                                     counter::getAndIncrement,
                                                     (lts) -> run.pdSelector.pd(lts, run.schemaSpec),
                                                     ltsToSkip,
                                                     pdsToSkip)) ;
                }
                else
                {
                    visitors.add(visitor);
                }
            }

            Set<Long> partitions = new HashSet<>();
            for (long i = 0; i < maxLts; i++)
                partitions.add(run.pdSelector.pd(i, run.schemaSpec));

            // Step one: figure out which partitions we can skip while still keeping it reproducible
            for (Long pdToCheck : partitions)
            {
                if (pdsToSkip.contains(pdToCheck))
                    continue;
                pdsToSkip.add(pdToCheck);
                Runner.init(configuration, run);

                try
                {
                    runOnce(visitors, maxLts);
                    System.out.println("Can not skip " + pdToCheck + "\nCan only skip these: " + toString(pdsToSkip));
                    pdsToSkip.remove(pdToCheck);
                }
                catch (RuntimeException t)
                {
                    if (check.test(t))
                    {
                        System.out.printf("Safe to skip: %d because without it we're still hitting an exception %s.\n%s\n",
                                          pdToCheck,
                                          t.getMessage(),
                                          toString(pdsToSkip));
                    }
                    else
                    {
                        System.out.println("Can not skip " + pdToCheck + "\n, since we seem to repro a different issue. Can only skip these: " + toString(pdsToSkip));
                        pdsToSkip.remove(pdToCheck);
                    }
                }
                run.sut.schemaChange("DROP KEYSPACE " + run.schemaSpec.keyspace);
            }

            // Step two: figure out which lts can be skipped within the remaining partitions
            for (long lts = 0; lts < maxLts; lts++)
            {
                long ltsToCheck = lts;
                if (ltsToSkip.contains(ltsToCheck) || pdsToSkip.contains(run.pdSelector.pd(lts, run.schemaSpec)))
                    continue;
                ltsToSkip.add(ltsToCheck);
                Runner.init(configuration, run);

                try
                {
                    runOnce(visitors, maxLts);
                    System.out.println("Can not skip " + ltsToCheck + "\nCan only skip these: " + toString(ltsToSkip));
                    ltsToSkip.remove(ltsToCheck);
                }
                catch (RuntimeException t)
                {
                    if (check.test(t))
                    {
                        System.out.printf("Safe to skip: %d because without it we're still hitting an exception %s.\n%s\n",
                                          ltsToCheck,
                                          t.getMessage(),
                                          toString(ltsToSkip));
                    }
                    else
                    {
                        System.out.println("Can not skip " + lts + "\n, since we seem to repro a different issue. Can only skip these: " + toString(ltsToSkip));
                        ltsToSkip.remove(ltsToCheck);
                    }

                }
                run.sut.schemaChange("DROP KEYSPACE " + run.schemaSpec.keyspace);
            }
        }
        catch (Throwable t)
        {
            System.out.println(t.getMessage());
            t.printStackTrace();
        }
        finally
        {
            System.exit(1);
        }
    }

    public static void runOnce(List<Visitor> visitors, long maxLts)
    {
        for (long lts = 0; lts <= maxLts; lts++)
        {
            for (Visitor visitor : visitors)
            {
                visitor.visit();
            }
        }
    }

    public static String toString(Set<Long> longs)
    {
        if (longs.isEmpty())
            return "";

        String s = "";
        for (Long aLong : longs)
        {
            s += aLong + "L,";
        }
        return s.substring(0, s.length() - 1);
    }
}
