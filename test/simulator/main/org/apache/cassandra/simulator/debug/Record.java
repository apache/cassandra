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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.simulator.ClusterSimulation;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.concurrent.Threads;

import static org.apache.cassandra.io.util.File.WriteMode.OVERWRITE;
import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;

public class Record
{
    private static final Logger logger = LoggerFactory.getLogger(Record.class);
    private static final Pattern NORMALISE_THREAD_RECORDING_OUT = Pattern.compile("(Thread\\[[^]]+:[0-9]+),[0-9](,node[0-9]+)_[0-9]+]");
    private static final Pattern NORMALISE_LAMBDA = Pattern.compile("((\\$\\$Lambda\\$[0-9]+/[0-9]+)?(@[0-9a-f]+)?)");

    public static void record(String saveToDir, long seed, boolean withRng, boolean withRngCallSites, ClusterSimulation.Builder<?> builder)
    {
        File eventFile = new File(new File(saveToDir), Long.toHexString(seed) + ".gz");
        File rngFile = new File(new File(saveToDir), Long.toHexString(seed) + ".rng.gz");

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

        try (PrintWriter eventOut = new PrintWriter(new GZIPOutputStream(eventFile.newOutputStream(OVERWRITE), 1 << 16));
             DataOutputStreamPlus rngOut = new BufferedDataOutputStreamPlus(Channels.newChannel(withRng ? new GZIPOutputStream(rngFile.newOutputStream(OVERWRITE), 1 << 16) : new ByteArrayOutputStream(0))))
        {
            eventOut.println("modifiers:"
                             + (withRng ? "rng," : "") + (withRngCallSites ? "rngCallSites," : "")
                             + (builder.capture().waitSites ? "waitSites," : "") + (builder.capture().wakeSites ? "wakeSites," : ""));

            RandomSourceRecorder random;
            if (withRng)
            {
                random = new RandomSourceRecorder(rngOut, new RandomSource.Default(), withRngCallSites);
                builder.random(random);
            }
            else
            {
                random = null;
            }

            // periodic forced flush to ensure state is on disk after some kind of stall
            Thread flusher = new Thread(() -> {
                try
                {
                    while (true)
                    {
                        Thread.sleep(1000);
                        eventOut.flush();
                        if (random != null)
                        {
                            synchronized (random)
                            {
                                rngOut.flush();
                            }
                        }
                    }
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
                catch (InterruptedException ignore)
                {
                }
                finally
                {
                    eventOut.flush();
                    try
                    {
                        if (random != null)
                        {
                            synchronized (random)
                            {
                                rngOut.flush();
                            }
                        }
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }, "Flush Recordings of " + seed);
            flusher.setDaemon(true);
            flusher.start();

            try (ClusterSimulation<?> cluster = builder.create(seed))
            {
                try (CloseableIterator<?> iter = cluster.simulation.iterator();)
                {
                    while (iter.hasNext())
                        eventOut.println(normaliseRecordingOut(iter.next().toString()));

                    if (random != null)
                        random.close();
                }
                finally
                {
                    eventOut.flush();
                    rngOut.flush();
                }
            }
            finally
            {
                flusher.interrupt();
            }
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            throw new RuntimeException("Failed on seed " + Long.toHexString(seed), t);
        }
    }

    private static String normaliseRecordingOut(String input)
    {
        return NORMALISE_THREAD_RECORDING_OUT.matcher(
            NORMALISE_LAMBDA.matcher(input).replaceAll("")
        ).replaceAll("$1$2]");
    }

    public static class RandomSourceRecorder extends RandomSource.Abstract implements Supplier<RandomSource>, Closeable
    {
        private static final AtomicReferenceFieldUpdater<RandomSourceRecorder, Thread> lockedUpdater = AtomicReferenceFieldUpdater.newUpdater(Record.RandomSourceRecorder.class, Thread.class, "locked");

        final DataOutputStreamPlus out;
        final AbstractRecorder threads;
        final RandomSource wrapped;
        int count = 0;
        volatile Thread locked;
        volatile boolean disabled;

        public RandomSourceRecorder(DataOutputStreamPlus out, RandomSource wrapped, boolean withCallSites)
        {
            this.out = out;
            this.wrapped = wrapped;
            this.threads = new AbstractRecorder(out, withCallSites);
        }

        private void enter()
        {
            while (!lockedUpdater.compareAndSet(this, null, Thread.currentThread()))
            {
                if (disabled)
                    return;

                Thread alt = locked;
                if (alt == null)
                    continue;
                StackTraceElement[] altTrace = alt.getStackTrace();
                if (Stream.of(altTrace).noneMatch(ste -> ste.getClassName().equals(RandomSourceRecorder.class.getName())))
                    continue;

                disabled = true;
                logger.error("Race within RandomSourceReconciler between {} and {} - means we have a Simulator bug permitting two threads to run at once\n{}", Thread.currentThread(), alt, Threads.prettyPrint(altTrace, true, "\n"));
                throw failWithOOM();
            }
        }

        private void exit()
        {
            locked = null;
        }

        public int uniform(int min, int max)
        {
            int v = wrapped.uniform(min, max);
            if (disabled)
                return v;

            enter();
            try
            {
                synchronized (this)
                {
                    out.writeByte(1);
                    out.writeVInt(count++);
                    threads.writeThread();
                    out.writeVInt(min);
                    out.writeVInt(max - min);
                    out.writeVInt(v - min);
                }
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
                synchronized (this)
                {
                    out.writeByte(2);
                    out.writeVInt(count++);
                    threads.writeThread();
                    out.writeVInt(min);
                    out.writeVInt(max - min);
                    out.writeVInt(v - min);
                }
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
                synchronized (this)
                {
                    out.writeByte(3);
                    out.writeVInt(count++);
                    threads.writeThread();
                    out.writeFloat(v);
                }
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

        public double uniformDouble()
        {
            double v = wrapped.uniformDouble();
            if (disabled)
                return v;

            enter();
            try
            {
                synchronized (this)
                {
                    out.writeByte(6);
                    out.writeVInt(count++);
                    threads.writeThread();
                    out.writeDouble(v);
                }
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

        public void reset(long seed)
        {
            wrapped.reset(seed);
            if (disabled)
                return;

            enter();
            try
            {
                synchronized (this)
                {
                    out.writeByte(4);
                    out.writeVInt(count++);
                    out.writeVInt(seed);
                }
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

        public long reset()
        {
            long v = wrapped.reset();
            if (disabled)
                return v;

            enter();
            try
            {
                synchronized (this)
                {
                    out.writeByte(5);
                    out.writeVInt(count++);
                    out.writeFloat(v);
                }
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

        public RandomSource get()
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

    public static class AbstractRecorder
    {
        final DataOutputStreamPlus out;
        final boolean withCallSites;
        final Map<Object, Integer> objects = new IdentityHashMap<>();

        public AbstractRecorder(DataOutputStreamPlus out, boolean withCallSites)
        {
            this.out = out;
            this.withCallSites = withCallSites;
        }

        public void writeThread() throws IOException
        {
            Thread thread = Thread.currentThread();
            writeInterned(thread);
            if (withCallSites)
            {
                StackTraceElement[] ste = thread.getStackTrace();
                String trace = Arrays.stream(ste, 3, ste.length)
                                     .filter(st -> !st.getClassName().equals("org.apache.cassandra.simulator.debug.Record")
                                                   && !st.getClassName().equals("org.apache.cassandra.simulator.SimulationRunner$Record"))
                                     .collect(new Threads.StackTraceCombiner(true, "", "\n", ""));
                out.writeUTF(trace);
            }
        }

        public void writeInterned(Object o) throws IOException
        {
            Integer id = objects.get(o);
            if (id != null)
            {
                out.writeVInt(id);
            }
            else
            {
                out.writeVInt(objects.size());
                out.writeUTF(o.toString());
                objects.put(o, objects.size());
            }
        }
    }
}
