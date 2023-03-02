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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.simulator.ClusterSimulation;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.SimulationRunner.RecordOption;
import org.apache.cassandra.simulator.systems.InterceptedWait.CaptureSites.Capture;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.concurrent.Threads;

import static org.apache.cassandra.simulator.SimulationRunner.RecordOption.NONE;
import static org.apache.cassandra.simulator.SimulationRunner.RecordOption.VALUE;
import static org.apache.cassandra.simulator.SimulationRunner.RecordOption.WITH_CALLSITES;
import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;

public class Reconcile
{
    private static final Logger logger = LoggerFactory.getLogger(Reconcile.class);

    private static final Pattern STRIP_TRACES = Pattern.compile("(Wakeup|Continue|Timeout|Waiting)\\[(((([a-zA-Z]\\.)*[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_<>$]+:[\\-0-9]+; )*(([a-zA-Z]\\.)*[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_<>$]+:[\\-0-9]+))( #\\[.*?]#)?) ?(by\\[.*?])?]");
    private static final Pattern STRIP_NOW_TRACES = Pattern.compile("( #\\[.*?]#)");
    private static final Pattern NORMALISE_THREAD_RECORDING_IN = Pattern.compile("(Thread\\[[^]]+:[0-9]+),?[0-9]+(,node[0-9]+)]");
    static final Pattern NORMALISE_LAMBDA = Pattern.compile("((\\$\\$Lambda\\$[0-9]+/[0-9]+)?(@[0-9a-f]+)?)");
    static final Pattern NORMALISE_THREAD = Pattern.compile("(Thread\\[[^]]+:[0-9]+),[0-9](,node[0-9]+)(_[0-9]+)?]");

    public static class AbstractReconciler
    {
        private static final Logger logger = LoggerFactory.getLogger(AbstractReconciler.class);

        final DataInputPlus in;
        final List<String> strings = new ArrayList<>();
        final boolean inputHasCallSites;
        final boolean reconcileCallSites;
        int line;

        public AbstractReconciler(DataInputPlus in, boolean inputHasCallSites, RecordOption reconcile)
        {
            this.in = in;
            this.inputHasCallSites = inputHasCallSites;
            this.reconcileCallSites = reconcile == WITH_CALLSITES;
        }

        String readInterned() throws IOException
        {
            int id = in.readVInt32();
            if (id == strings.size()) strings.add(in.readUTF());
            else if (id > strings.size()) throw failWithOOM();
            return strings.get(id);
        }

        private String readCallSite() throws IOException
        {
            if (!inputHasCallSites)
                return "";

            String trace = in.readUTF();
            for (int i = trace.indexOf('\n') ; i >= 0 ; i = trace.indexOf('\n', i + 1))
                ++line;
            return reconcileCallSites ? trace : "";
        }

        private String ourCallSite()
        {
            if (!reconcileCallSites)
                return "";

            StackTraceElement[] ste = Thread.currentThread().getStackTrace();
            return Arrays.stream(ste, 4, ste.length)
                         .filter(st -> !st.getClassName().equals("org.apache.cassandra.simulator.debug.Reconcile")
                                       && !st.getClassName().equals("org.apache.cassandra.simulator.SimulationRunner$Reconcile")
                                       && !st.getClassName().equals("sun.reflect.NativeMethodAccessorImpl") // depends on async compile thread
                                       && !st.getClassName().startsWith("sun.reflect.GeneratedMethodAccessor")) // depends on async compile thread
                         .collect(new Threads.StackTraceCombiner(true, "", "\n", ""));
        }

        public void checkThread() throws IOException
        {
            // normalise lambda also strips Object.toString() inconsistencies for some Thread objects
            String thread = NORMALISE_LAMBDA.matcher(readInterned()).replaceAll("");
            String ourThread = NORMALISE_LAMBDA.matcher(Thread.currentThread().toString()).replaceAll("");
            String callSite = NORMALISE_LAMBDA.matcher(readCallSite()).replaceAll("");
            String ourCallSite = NORMALISE_LAMBDA.matcher(ourCallSite()).replaceAll("");
            if (!thread.equals(ourThread) || !callSite.equals(ourCallSite))
            {
                logger.error(String.format("(%s,%s) != (%s,%s)", thread, callSite, ourThread, ourCallSite));
                throw failWithOOM();
            }
        }
    }

    public static class TimeReconciler extends AbstractReconciler implements SimulatedTime.Listener, Closeable
    {
        boolean disabled;

        public TimeReconciler(DataInputPlus in, boolean inputHasCallSites, RecordOption reconcile)
        {
            super(in, inputHasCallSites, reconcile);
        }

        @Override
        public void close()
        {
            disabled = true;
        }

        @Override
        public synchronized void accept(String kind, long value)
        {
            if (disabled)
                return;

            try
            {
                String testKind = readInterned();
                long testValue = in.readUnsignedVInt();
                checkThread();
                if (!kind.equals(testKind) || value != testValue)
                {
                    logger.error("({},{}) != ({},{})", kind, value, testKind, testValue);
                    throw failWithOOM();
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static class RandomSourceReconciler extends RandomSource.Abstract implements Supplier<RandomSource>, Closeable
    {
        private static final Logger logger = LoggerFactory.getLogger(RandomSourceReconciler.class);
        private static final AtomicReferenceFieldUpdater<Reconcile.RandomSourceReconciler, Thread> lockedUpdater = AtomicReferenceFieldUpdater.newUpdater(Reconcile.RandomSourceReconciler.class, Thread.class, "locked");
        final DataInputPlus in;
        final RandomSource wrapped;
        final AbstractReconciler threads;
        int count;
        volatile Thread locked;
        volatile boolean disabled;

        public RandomSourceReconciler(DataInputPlus in, RandomSource wrapped, boolean inputHasCallSites, RecordOption reconcile)
        {
            this.in = in;
            this.wrapped = wrapped;
            this.threads = new AbstractReconciler(in, inputHasCallSites, reconcile);
        }

        private void enter()
        {
            if (!lockedUpdater.compareAndSet(this, null, Thread.currentThread()))
            {
                if (disabled)
                    return;

                disabled = true;
                logger.error("Race within RandomSourceReconciler - means we have a Simulator bug permitting two threads to run at once");
                throw failWithOOM();
            }
        }

        private void exit()
        {
            locked = null;
        }

        public void onDeterminismCheck(long value)
        {
            if (disabled)
                return;

            enter();
            try
            {
                byte type = in.readByte();
                int c = in.readVInt32();
                long v = in.readLong();
                threads.checkThread();
                if (type != 7 || c != count || value != v)
                {
                    logger.error(String.format("(%d,%d,%d) != (%d,%d,%d)", 7, count, value, type, c, v));
                    throw failWithOOM();
                }
                ++count;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                exit();
            }
        }

        public int uniform(int min, int max)
        {
            int v = wrapped.uniform(min, max);
            if (disabled)
                return v;

            enter();
            try
            {
                byte type = in.readByte();
                int c = in.readVInt32();
                threads.checkThread();
                int min1 = in.readVInt32();
                int max1 = in.readVInt32() + min1;
                int v1 = in.readVInt32() + min1;
                if (type != 1 || min != min1 || max != max1 || v != v1 || c != count)
                {
                    logger.error(String.format("(%d,%d,%d[%d,%d]) != (%d,%d,%d[%d,%d])", 1, count, v, min, max, type, c, v1, min1, max1));
                    throw failWithOOM();
                }
                ++count;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                exit();
            }
            return v;
        }

        public long uniform(long min, long max)
        {
            long v = wrapped.uniform(min, max);
            if (disabled)
                return v;

            enter();
            try
            {
                byte type = in.readByte();
                int c = in.readVInt32();
                threads.checkThread();
                long min1 = in.readVInt();
                long max1 = in.readVInt() + min1;
                long v1 = in.readVInt() + min1;
                if (type != 2 || min != min1 || max != max1 || v != v1 || c != count)
                {
                    logger.error(String.format("(%d,%d,%d[%d,%d]) != (%d,%d,%d[%d,%d])", 2, count, v, min, max, type, c, v1, min1, max1));
                    throw failWithOOM();
                }
                ++count;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                exit();
            }
            return v;
        }

        public float uniformFloat()
        {
            float v = wrapped.uniformFloat();
            if (disabled)
                return v;

            enter();
            try
            {
                byte type = in.readByte();
                int c = in.readVInt32();
                threads.checkThread();
                float v1 = in.readFloat();
                if (type != 3 || v != v1 || c != count)
                {
                    logger.error(String.format("(%d,%d,%f) != (%d,%d,%f)", 3, count, v, type, c, v1));
                    throw failWithOOM();
                }
                ++count;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                exit();
            }
            return v;
        }

        @Override
        public double uniformDouble()
        {
            double v = wrapped.uniformDouble();
            if (disabled)
                return v;

            enter();
            try
            {
                byte type = in.readByte();
                int c = in.readVInt32();
                threads.checkThread();
                double v1 = in.readDouble();
                if (type != 6 || v != v1 || c != count)
                {
                    logger.error(String.format("(%d,%d,%f) != (%d,%d,%f)", 6, count, v, type, c, v1));
                    throw failWithOOM();
                }
                ++count;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                exit();
            }
            return v;
        }

        public synchronized void reset(long seed)
        {
            wrapped.reset(seed);
            if (disabled)
                return;

            enter();
            try
            {
                byte type = in.readByte();
                int c = in.readVInt32();
                long v1 = in.readVInt();
                if (type != 4 || seed != v1 || c != count)
                    throw failWithOOM();
                ++count;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                exit();
            }
        }

        public synchronized long reset()
        {
            long v = wrapped.reset();
            if (disabled)
                return v;

            enter();
            try
            {
                byte type = in.readByte();
                int c = in.readVInt32();
                long v1 = in.readVInt();
                if (type != 5 || v != v1 || c != count)
                    throw failWithOOM();
                ++count;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                exit();
            }
            return v;
        }

        public synchronized RandomSource get()
        {
            if (count++ > 0)
                throw failWithOOM();
            return this;
        }

        @Override
        public void close()
        {
            disabled = true;
        }
    }

    public static void reconcileWith(String loadFromDir, long seed, RecordOption withRng, RecordOption withTime, ClusterSimulation.Builder<?> builder)
    {
        File eventFile = new File(new File(loadFromDir), Long.toHexString(seed) + ".gz");
        File rngFile = new File(new File(loadFromDir), Long.toHexString(seed) + ".rng.gz");
        File timeFile = new File(new File(loadFromDir), Long.toHexString(seed) + ".time.gz");

        try (BufferedReader eventIn = new BufferedReader(new InputStreamReader(new GZIPInputStream(eventFile.newInputStream())));
             DataInputStreamPlus rngIn = Util.DataInputStreamPlusImpl.wrap(rngFile.exists() && withRng != NONE ? new GZIPInputStream(rngFile.newInputStream()) : new ByteArrayInputStream(new byte[0]));
             DataInputStreamPlus timeIn = Util.DataInputStreamPlusImpl.wrap(timeFile.exists() && withTime != NONE ? new GZIPInputStream(timeFile.newInputStream()) : new ByteArrayInputStream(new byte[0])))
        {
            boolean inputHasWaitSites, inputHasWakeSites, inputHasRngCallSites, inputHasTimeCallSites;
            {
                String modifiers = eventIn.readLine();
                if (!modifiers.startsWith("modifiers:"))
                    throw new IllegalStateException();

                builder.capture(new Capture(
                    builder.capture().waitSites & (inputHasWaitSites = modifiers.contains("waitSites")),
                    builder.capture().wakeSites & (inputHasWakeSites = modifiers.contains("wakeSites")),
                    builder.capture().nowSites)
                );
                inputHasRngCallSites = modifiers.contains("rngCallSites");
                if (!modifiers.contains("rng")) withRng = NONE;
                if (withRng == WITH_CALLSITES && !inputHasRngCallSites) withRng = VALUE;

                inputHasTimeCallSites = modifiers.contains("timeCallSites");
                if (!modifiers.contains("time")) withTime = NONE;
                if (withTime == WITH_CALLSITES && !inputHasTimeCallSites) withTime = VALUE;
            }
            if (withRng != NONE && !rngFile.exists())
                throw new IllegalStateException();
            if (withTime != NONE && !timeFile.exists())
                throw new IllegalStateException();

            {
                Set<String> modifiers = new LinkedHashSet<>();
                if (withRng == WITH_CALLSITES)
                    modifiers.add("rngCallSites");
                else if (withRng == VALUE)
                    modifiers.add("rng");
                if (withTime == WITH_CALLSITES)
                    modifiers.add("timeCallSites");
                else if (withTime == VALUE)
                    modifiers.add("time");
                if (builder.capture().waitSites)
                    modifiers.add("WaitSites");
                if (builder.capture().wakeSites)
                    modifiers.add("WakeSites");
                logger.error("Seed 0x{} ({}) (With: {})", Long.toHexString(seed), eventFile, modifiers);
            }

            RandomSourceReconciler random = null;
            TimeReconciler time = null;
            if (withRng != NONE)
            {
                builder.random(random = new RandomSourceReconciler(rngIn, new RandomSource.Default(), inputHasRngCallSites, withRng));
                builder.onThreadLocalRandomCheck(random::onDeterminismCheck);
            }
            if (withTime != NONE)
                builder.timeListener(time = new TimeReconciler(timeIn, inputHasTimeCallSites, withTime));

            class Line { int line = 1; } Line line = new Line(); // box for heap dump analysis
            try (ClusterSimulation<?> cluster = builder.create(seed);
                 CloseableIterator<?> iter = cluster.simulation.iterator())
            {
                try
                {
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
                    if (random != null)
                        random.close();
                    if (time != null)
                        time.close();
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    throw t;
                }
            }
        }
        catch (Throwable t)
        {
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
        throw failWithOOM();
    }
}
