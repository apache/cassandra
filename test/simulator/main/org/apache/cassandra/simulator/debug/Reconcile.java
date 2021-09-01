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

package org.apache.cassandra.simulator.debug;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.simulator.ClusterSimulation;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.utils.concurrent.Threads;

import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;

public class Reconcile
{
    private static final Logger logger = LoggerFactory.getLogger(Reconcile.class);

    private static final Pattern STRIP_TRACES = Pattern.compile("(Wakeup|Continue|Timeout|Waiting)\\[(((([a-zA-Z]\\.)*[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_<>$]+:[\\-0-9]+; )*(([a-zA-Z]\\.)*[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_<>$]+:[\\-0-9]+))( #\\[.*?]#)?) ?(by\\[.*?])?]");
    private static final Pattern STRIP_NOW_TRACES = Pattern.compile("( #\\[.*?]#)");
    private static final Pattern NORMALISE_THREAD_RECORDING_IN = Pattern.compile("(Thread\\[[^]]+:[0-9]+)(,node[0-9]+)]");
    static final Pattern NORMALISE_LAMBDA = Pattern.compile("((\\$\\$Lambda\\$[0-9]+/[0-9]+)?(@[0-9a-f]+)?)");
    static final Pattern NORMALISE_THREAD = Pattern.compile("(Thread\\[[^]]+:[0-9]+),[0-9](,node[0-9]+)(_[0-9]+)?]");

    public static class AbstractReconciler
    {
        private static final Logger logger = LoggerFactory.getLogger(AbstractReconciler.class);

        final DataInputPlus in;
        final List<String> strings = new ArrayList<>();
        final boolean inputHasCallSites;
        final boolean reconcileCallSites;

        public AbstractReconciler(DataInputPlus in, boolean inputHasCallSites, boolean reconcileCallSites)
        {
            this.in = in;
            this.inputHasCallSites = inputHasCallSites;
            this.reconcileCallSites = reconcileCallSites;
        }

        String readInterned() throws IOException
        {
            int id = (int) in.readVInt();
            if (id == strings.size()) strings.add(in.readUTF());
            else if (id > strings.size()) failWithOOM();
            return strings.get(id);
        }

        private String readCallSite() throws IOException
        {
            if (!inputHasCallSites)
                return "";

            String trace = in.readUTF();
            return reconcileCallSites ? trace : "";
        }

        private String ourCallSite()
        {
            if (!reconcileCallSites)
                return "";

            StackTraceElement[] ste = Thread.currentThread().getStackTrace();
            return Arrays.stream(ste, 3, ste.length)
                         .filter(st -> !st.getClassName().equals("org.apache.cassandra.simulator.paxos.PaxosBurnTest"))
                         .collect(new Threads.StackTraceCombiner(true, "", "\n", ""));
        }

        public void checkThread() throws IOException
        {
            String thread = readInterned();
            String ourThread = Thread.currentThread().toString();
            String callSite = readCallSite();
            String ourCallSite = ourCallSite();
            if (!thread.equals(ourThread) || !callSite.equals(ourCallSite))
            {
                logger.error(String.format("(%s,%s) != (%s,%s)", thread, callSite, ourThread, ourCallSite));
                failWithOOM();
            }
        }
    }

    public static class RandomSourceReconciler extends RandomSource.Abstract implements Supplier<RandomSource>
    {
        private static final Logger logger = LoggerFactory.getLogger(RandomSourceReconciler.class);

        final DataInputPlus in;
        final RandomSource wrapped;
        final AbstractReconciler threads;
        int count;

        public RandomSourceReconciler(DataInputPlus in, RandomSource wrapped, boolean inputHasCallSites, boolean reconcileCallSites)
        {
            this.in = in;
            this.wrapped = wrapped;
            this.threads = new AbstractReconciler(in, inputHasCallSites, reconcileCallSites);
        }

        public int uniform(int min, int max)
        {
            int v = wrapped.uniform(min, max);
            try
            {
                byte type = in.readByte();
                int c = (int) in.readVInt();
                threads.checkThread();
                int min1 = (int) in.readVInt();
                int max1 = (int) in.readVInt() + min1;
                int v1 = (int) in.readVInt() + min1;
                if (type != 1 || min != min1 || max != max1 || v != v1 || c != count)
                {
                    logger.error(String.format("(%d,%d,%d[%d,%d]) != (%d,%d,%d[%d,%d])", 1, count, v, min, max, type, c, v1, min1, max1));
                    failWithOOM();
                }
                ++count;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return v;
        }

        public long uniform(long min, long max)
        {
            long v = wrapped.uniform(min, max);
            try
            {
                byte type = in.readByte();
                int c = (int) in.readVInt();
                threads.checkThread();
                long min1 = in.readVInt();
                long max1 = in.readVInt() + min1;
                long v1 = in.readVInt() + min1;
                if (type != 2 || min != min1 || max != max1 || v != v1 || c != count)
                {
                    logger.error(String.format("(%d,%d,%d[%d,%d]) != (%d,%d,%d[%d,%d])", 2, count, v, min, max, type, c, v1, min1, max1));
                    failWithOOM();
                }
                ++count;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return v;
        }

        public float uniformFloat()
        {
            float v = wrapped.uniformFloat();
            try
            {
                byte type = in.readByte();
                int c = (int) in.readVInt();
                threads.checkThread();
                float v1 = in.readFloat();
                if (type != 3 || v != v1 || c != count)
                {
                    logger.error(String.format("(%d,%d,%f) != (%d,%d,%f)", 3, count, v, type, c, v1));
                    failWithOOM();
                }
                ++count;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return v;
        }

        @Override
        public double uniformDouble()
        {
            double v = wrapped.uniformDouble();
            try
            {
                byte type = in.readByte();
                int c = (int) in.readVInt();
                threads.checkThread();
                double v1 = in.readDouble();
                if (type != 6 || v != v1 || c != count)
                {
                    logger.error(String.format("(%d,%d,%f) != (%d,%d,%f)", 6, count, v, type, c, v1));
                    failWithOOM();
                }
                ++count;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return v;
        }

        public synchronized void reset(long seed)
        {
            wrapped.reset(seed);
            try
            {
                byte type = in.readByte();
                int c = (int) in.readVInt();
                long v1 = in.readVInt();
                if (type != 4 || seed != v1 || c != count)
                    failWithOOM();
                ++count;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public synchronized long reset()
        {
            long v = wrapped.reset();
            try
            {
                byte type = in.readByte();
                int c = (int) in.readVInt();
                long v1 = in.readVInt();
                if (type != 5 || v != v1 || c != count)
                    failWithOOM();
                ++count;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return v;
        }

        public synchronized RandomSource get()
        {
            if (count++ > 0)
                failWithOOM();
            return this;
        }
    }

    public static void reconcileWith(String loadFromDir, long seed, boolean withRng, boolean withRngCallSites, ClusterSimulation.Builder<?> builder)
    {
        if (withRngCallSites && !withRng)
            throw new IllegalStateException();

        File eventFile = new File(new File(loadFromDir), Long.toHexString(seed) + ".gz");
        File rngFile = new File(new File(loadFromDir), Long.toHexString(seed) + ".rng.gz");

        try (BufferedReader eventIn = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(eventFile))));
             DataInputPlus.DataInputStreamPlus rngIn = new DataInputPlus.DataInputStreamPlus(rngFile.exists() ? new GZIPInputStream(new FileInputStream(rngFile)) : new ByteArrayInputStream(new byte[0])))
        {
            boolean inputHasWaitSites, inputHasWakeSites, inputHasRngCallSites;
            {
                String modifiers = eventIn.readLine();
                if (!modifiers.startsWith("modifiers:"))
                    throw new IllegalStateException();

                builder.capture(new Capture(
                    builder.capture().waitSites & (inputHasWaitSites = modifiers.contains("waitSites")),
                    builder.capture().wakeSites & (inputHasWakeSites = modifiers.contains("wakeSites")),
                    builder.capture().nowSites)
                );
                withRng &= modifiers.contains("rng");
                withRngCallSites &= inputHasRngCallSites = modifiers.contains("rngCallSites");
            }
            if (withRng && !rngFile.exists())
                throw new IllegalStateException();

            {
                Set<String> modifiers = new LinkedHashSet<>();
                if (withRngCallSites)
                    modifiers.add("rngCallSites");
                else if (withRng)
                    modifiers.add("rng");
                if (builder.capture().waitSites)
                    modifiers.add("WaitSites");
                if (builder.capture().wakeSites)
                    modifiers.add("WakeSites");
                logger.error("Seed 0x{} ({}) (With: {})", Long.toHexString(seed), eventFile, modifiers);
            }

            if (withRng)
                builder.random(new RandomSourceReconciler(rngIn, new RandomSource.Default(), inputHasRngCallSites, withRngCallSites));

            try (ClusterSimulation<?> cluster = builder.create(seed))
            {
                class Line { int line = 1; } Line line = new Line(); // box for heap dump analysis
                Iterable<?> iterable = cluster.simulation.iterable();
                Iterator<?> iter = iterable.iterator();
                while (iter.hasNext())
                {
                    ++line.line;
                    String rawInput = eventIn.readLine();
                    String input = (inputHasWaitSites != builder.capture().waitSites || inputHasWakeSites != builder.capture().wakeSites)
                                   ? normaliseRecordingInWithoutWaitOrWakeSites(rawInput, inputHasWaitSites && !builder.capture().waitSites, inputHasWakeSites && !builder.capture().wakeSites)
                                   : normaliseRecordingIn(rawInput);
                    Object next = iter.next();
                    String rawOutput = next.toString();
                    String output = normaliseReconcileWithRecording(rawOutput);
                    if (!input.equals(output))
                        failWithHeapDump(line.line, input, output);
                }
            }
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            if (t instanceof Error)
                throw (Error) t;
            throw new RuntimeException("Failed on seed " + Long.toHexString(seed), t);
        }
    }


    private static String normaliseRecordingIn(String input)
    {
        return STRIP_NOW_TRACES.matcher(
            NORMALISE_THREAD_RECORDING_IN.matcher(
                NORMALISE_LAMBDA.matcher(input).replaceAll("")
            ).replaceAll("$1$2]")
        ).replaceAll("");
    }

    private static String normaliseRecordingInWithoutWaitOrWakeSites(String input, boolean stripWaitSites, boolean stripWakeSites)
    {
        return STRIP_TRACES.matcher(
            NORMALISE_THREAD_RECORDING_IN.matcher(
                NORMALISE_LAMBDA.matcher(input).replaceAll("")
            ).replaceAll("$1$2]")
        ).replaceAll(stripWaitSites && stripWakeSites ? "$1[]" : stripWaitSites ? "$1[$9]" : "$1[$3]");
    }

    private static String normaliseReconcileWithRecording(String input)
    {
        return STRIP_NOW_TRACES.matcher(
            NORMALISE_THREAD.matcher(
                NORMALISE_LAMBDA.matcher(input).replaceAll("")
            ).replaceAll("$1$2]")
        ).replaceAll("");
    }

    static void failWithHeapDump(int line, Object input, Object output)
    {
        logger.error("Line {}", line);
        logger.error("Input {}", input);
        logger.error("Output {}", output);
        failWithOOM();
    }
}
