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

package org.apache.cassandra.simulator.paxos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntSet;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.utils.Clock;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.Assertions;

import static org.apache.commons.lang3.ArrayUtils.add;
import static org.apache.commons.lang3.ArrayUtils.swap;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Notes:
 * * anomalyDirtyRead was left out as Accord doesn't reject requests, so without a way to reject or abort
 *   requests then client doesn't have any way to abserve a REJECT, so all issues are UNKNOWN.
 *
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING) // since Random is used, make sure tests run in a determanistic order
public class HistoryValidatorTest
{
    private static final Logger logger = LoggerFactory.getLogger(HistoryValidatorTest.class);
    private static final Random RANDOM = random();
    private static final int[] PARTITIONS = IntStream.range(0, 10).toArray();
    private static final int x = 1;
    private static final int y = 2;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        List<Object[]> tests = new ArrayList<>();
        tests.add(test(LinearizabilityValidator.Factory.instance));
        tests.add(test(StrictSerializabilityValidator.Factory.instance));
        return tests;
    }

    private static Object[] test(HistoryValidator.Factory factory)
    {
        return new Object[]{ factory };
    }

    private final HistoryValidator.Factory factory;

    public HistoryValidatorTest(HistoryValidator.Factory factory)
    {
        this.factory = factory;
    }

    @Test
    public void orderingWithWriteTimeout()
    {
        IntSet timeoutEvents = set(4, 17, 83);
        for (boolean reject : Arrays.asList(true, false))
        {
            HistoryValidator validator = create();

            int logicalClock = 1;
            int[] seq = seq();
            for (int eventId = 0; eventId < 100; eventId++)
            {
                if (timeoutEvents.contains(eventId))
                {
                    if (!reject)
                        seq = add(seq, eventId); // wastn't observed, but was applied
                    continue;
                }
                single(validator, ob(eventId, ++logicalClock, ++logicalClock), 1, seq, true);
                seq = add(seq, eventId); //TODO forgot to add this and LinearizabilityValidator was success... should reject!
            }
        }
    }

    /**
     * This test differs from {@link #orderingWithWriteTimeout} as it defines event orders assuming
     * requests were concurrent, so may happen in different orderings.
     * <p>
     * This means that we may see the results out of order, but the sequence/count ordering will remain
     */
    @Test
    public void orderingWithWriteTimeoutWithConcurrency()
    {
        IntSet timeoutEvents = set(4, 17, 83);
        for (boolean reject : Arrays.asList(true, false))
        {
            HistoryValidator validator = create();
            // Since the requests are "concurrent" the window in which the operations happened are between start=1 and
            // end=responseOrder.
            int start = 1;
            int logicalClock = start;

            // 'ordering' is the order in which the txns are applied
            // 'indexOrder' is the order in which the events are seen; since requests are "concurrent" the order we
            //   validate may differ from the ordering they were applied.
            int[] ordering = IntStream.range(0, 100).toArray();
            if (reject)
                ordering = IntStream.of(ordering).filter(i -> !timeoutEvents.contains(i)).toArray();
            shuffle(ordering);
            int[] indexOrder = IntStream.range(0, ordering.length - 1).toArray();
            shuffle(indexOrder);
            for (int i = 0; i < indexOrder.length; i++)
            {
                int idx = indexOrder[i];
                int eventId = ordering[idx];
                if (timeoutEvents.contains(eventId))
                    continue;
                int[] seq = Arrays.copyOf(ordering, idx);
                single(validator, ob(eventId, start, ++logicalClock), 1, seq, true);
            }
        }
    }

    @Test
    public void anomalyNonMonotonicRead()
    {
        // Session1: w[x=10] -> Session2: r[x=10] -> r[x=0]
        test(dsl -> {
            dsl.txn(writeOnly(x));
            dsl.txn(readOnly(x, seq(0)));
            dsl.failingTxn(readOnly(x, seq())).isInstanceOf(HistoryViolation.class);
        });
    }

    @Test
    public void anomalyNonMonotonicWrite()
    {
        requiresMultiKeySupport();
        // Session1: w[x=10] -> w[y=10] -> Session2: r[y=10] -> r[x=0]
        test(dsl -> {
            dsl.txn(writeOnly(x));
            dsl.txn(writeOnly(y));
            dsl.txn(readOnly(y, seq(1)));
            dsl.failingTxn(readOnly(x, seq())).isInstanceOf(HistoryViolation.class);
        });
    }

    @Test
    public void anomalyNonMonotonicTransaction()
    {
        // Session1: r[x=5] -> w[y=10] -> Session2: r[y=10] -> r[x=0]
        requiresMultiKeySupport();
        test(dsl -> {
            dsl.txn(writeOnly(x), writeOnly(y));

            dsl.txn(readOnly(x, seq(0)));
            dsl.txn(writeOnly(y));
            dsl.txn(readOnly(y, seq(0, 2)));

            dsl.failingTxn(readOnly(x, seq())).isInstanceOf(HistoryViolation.class);
        });
    }

    @Test
    public void anomalyReadYourOwnWrites()
    {
        // This test is kinda a duplicate; here just for completness
        // w[x=12] -> r[x=8]
        test(dsl -> {
            dsl.txn(writeOnly(x));
            dsl.failingTxn(readOnly(x, seq())).isInstanceOf(HistoryViolation.class);
        });
    }

    //TODO write skew
    @Test
    public void anomalyReadSkew()
    {
        requiresMultiKeySupport();
        // two different txn are involved to make this happen
        // x=0, y=0
        // U1: starts
        // U2: starts
        // U1: r[x=0]
        // U2: w[x=5], w[y=5]
        // U2: commit
        // U1: r[y=5]
        // U1: commit
        HistoryValidator validator = create();

        // init
        txn(validator, ob(0, 1, 2), writeOnly(x), writeOnly(y));
        int u1 = 1, u1_start = 3, u1_end = 6;
        int u2 = 2, u2_start = 4, u2_end = 5;
        txn(validator, ob(u2, u2_start, u2_end), readWrite(x, seq(0)), readWrite(y, seq(0)));
        Assertions.assertThatThrownBy(() -> txn(validator, ob(u1, u1_start, u1_end), readWrite(x, seq(0)), readWrite(y, seq(0, u2))))
                  .isInstanceOf(HistoryViolation.class);
    }

    @Test
    public void anomalyWriteSkew()
    {
        // two different txn are involved to make this happen
        // x=0, y=0
        // U1: starts
        // U2: starts
        // U1: r[x=0]
    }

    @Test
    public void seenBehavior()
    {
        fromLog("Witness(start=4, end=7)\n" +
                "\tread(pk=121901541, id=2, count=0, seq=[])\n" +
                "\twrite(pk=121901541, id=2, success=true)\n" +

                "Witness(start=3, end=8)\n" +
                "\tread(pk=122950117, id=0, count=0, seq=[])\n" +
                "\twrite(pk=122950117, id=0, success=true)\n" +
                "\twrite(pk=119804389, id=0, success=true)\n" +

                "Witness(start=5, end=9)\n" +
                "\tread(pk=121901541, id=3, count=1, seq=[2])\n" +
                "\twrite(pk=121901541, id=3, success=true)\n" +

                "Witness(start=2, end=10)\n" +
                "\twrite(pk=122950117, id=1, success=true)\n" +
                "\twrite(pk=119804389, id=1, success=true)\n" +

                "Witness(start=6, end=11)\n" +
                "\tread(pk=121901541, id=4, count=2, seq=[2, 3])\n" +
                "\twrite(pk=121901541, id=4, success=true)\n" +

                "Witness(start=12, end=14)\n" +
                "\twrite(pk=121901541, id=5, success=true)\n" +

                "Witness(start=13, end=16)\n" +
                "\tread(pk=119804389, id=6, count=2, seq=[0, 1])\n" +
                "\twrite(pk=119804389, id=6, success=true)\n" +
                "\twrite(pk=122950117, id=6, success=true)\n" +

                "Witness(start=15, end=18)\n" +
                "\tread(pk=121901541, id=7, count=4, seq=[2, 3, 4, 5])\n" +
                "\twrite(pk=121901541, id=7, success=true)\n" +

                "Witness(start=17, end=20)\n" +
                "\tread(pk=119804389, id=8, count=3, seq=[0, 1, 6])\n" +
                "\twrite(pk=119804389, id=8, success=true)\n" +
                "\twrite(pk=122950117, id=8, success=true)\n" // this partition is what triggers
        );
    }

    private void requiresMultiKeySupport()
    {
        Assume.assumeTrue("Validator " + factory.getClass() + " does not support multi-key", factory instanceof StrictSerializabilityValidator.Factory);
    }

    private int[] shuffle(int[] ordering)
    {
        // shuffle array
        for (int i = ordering.length; i > 1; i--)
            swap(ordering, i - 1, RANDOM.nextInt(i));
        return ordering;
    }

    private static void txn(HistoryValidator validator, Observation ob, Event... events)
    {
        String type = events.length == 1 ? "single" : "multiple";
        logger.info("[Validator={}, Observation=({}, {}, {})] Validating {} {}}", validator.getClass().getSimpleName(), ob.id, ob.start, ob.end, type, events);
        try (HistoryValidator.Checker check = validator.witness(ob.start, ob.end))
        {
            for (Event e : events)
                e.process(ob, check);
        }
    }

    private static void single(HistoryValidator validator, Observation ob, int pk, int[] seq, boolean hasWrite)
    {
        txn(validator, ob, hasWrite ? readWrite(pk, seq) : readOnly(pk, seq));
    }

    private static Observation ob(int id, int start, int end)
    {
        // why empty result?  The users don't actually check the result's data, just existence
        return new Observation(id, QueryResults.empty(), start, end);
    }

    private static int[] seq(int... seq)
    {
        return seq;
    }

    private HistoryValidator create()
    {
        return factory.create(PARTITIONS);
    }

    private static IntSet set(int... values)
    {
        IntSet set = new IntHashSet(values.length);
        for (int v : values)
            set.add(v);
        return set;
    }

    private static Random random()
    {
        long seed = Long.parseLong(System.getProperty("cassandra.test.seed", Long.toString(Clock.Global.nanoTime())));
        logger.info("Random seed={}; set -Dcassandra.test.seed={} while reruning the tests to get the same order", seed, seed);
        return new Random(seed);
    }

    private static Event readWrite(int pk, int[] seq)
    {
        return new Event(EnumSet.of(Event.Type.READ, Event.Type.WRITE), pk, seq);
    }

    private static Event readOnly(int pk, int[] seq)
    {
        return new Event(EnumSet.of(Event.Type.READ), pk, seq);
    }

    private static Event writeOnly(int pk)
    {
        return new Event(EnumSet.of(Event.Type.WRITE), pk, null);
    }

    private void fromLog(String log)
    {
        IntSet pks = new IntHashSet();
        class Read
        {
            final int pk, id, count;
            final int[] seq;

            Read(int pk, int id, int count, int[] seq)
            {
                this.pk = pk;
                this.id = id;
                this.count = count;
                this.seq = seq;
            }
        }
        class Write
        {
            final int pk, id;
            final boolean success;

            Write(int pk, int id, boolean success)
            {
                this.pk = pk;
                this.id = id;
                this.success = success;
            }
        }
        class Witness
        {
            final int start, end;
            final List<Object> actions = new ArrayList<>();

            Witness(int start, int end)
            {
                this.start = start;
                this.end = end;
            }

            void read(int pk, int id, int count, int[] seq)
            {
                actions.add(new Read(pk, id, count, seq));
            }

            void write(int pk, int id, boolean success)
            {
                actions.add(new Write(pk, id, success));
            }

            void process(HistoryValidator validator)
            {
                try (HistoryValidator.Checker check = validator.witness(start, end))
                {
                    for (Object a : actions)
                    {
                        if (a instanceof Read)
                        {
                            Read read = (Read) a;
                            check.read(read.pk, read.id, read.count, read.seq);
                        }
                        else
                        {
                            Write write = (Write) a;
                            check.write(write.pk, write.id, write.success);
                        }
                    }
                }
            }
        }
        List<Witness> witnesses = new ArrayList<>();
        Witness current = null;
        for (String line : log.split("\n"))
        {
            if (line.startsWith("Witness"))
            {
                if (current != null)
                    witnesses.add(current);
                Matcher matcher = Pattern.compile("Witness\\(start=(.+), end=(.+)\\)").matcher(line);
                if (!matcher.find()) throw new AssertionError("Unable to match start/end of " + line);
                current = new Witness(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)));
            }
            else if (line.startsWith("\tread"))
            {
                Matcher matcher = Pattern.compile("\tread\\(pk=(.+), id=(.+), count=(.+), seq=\\[(.*)\\]\\)").matcher(line);
                if (!matcher.find()) throw new AssertionError("Unable to match read of " + line);
                int pk = Integer.parseInt(matcher.group(1));
                pks.add(pk);
                int id = Integer.parseInt(matcher.group(2));
                int count = Integer.parseInt(matcher.group(3));
                String seqStr = matcher.group(4);
                int[] seq = seqStr.isEmpty() ? new int[0] : Stream.of(seqStr.split(",")).map(String::trim).mapToInt(Integer::parseInt).toArray();
                current.read(pk, id, count, seq);
            }
            else if (line.startsWith("\twrite"))
            {
                Matcher matcher = Pattern.compile("\twrite\\(pk=(.+), id=(.+), success=(.+)\\)").matcher(line);
                if (!matcher.find()) throw new AssertionError("Unable to match write of " + line);
                int pk = Integer.parseInt(matcher.group(1));
                pks.add(pk);
                int id = Integer.parseInt(matcher.group(2));
                boolean success = Boolean.parseBoolean(matcher.group(3));
                current.write(pk, id, success);
            }
            else
            {
                throw new IllegalArgumentException("Unknow line: " + line);
            }
        }
        if (current != null)
            witnesses.add(current);
        int[] keys = pks.toArray();
        Arrays.sort(keys);
        HistoryValidator validator = factory.create(keys);
        for (Witness w : witnesses)
            w.process(validator);
    }

    private static class Event
    {
        enum Type
        {READ, WRITE}

        ;
        private final EnumSet<Type> types;
        private final int pk;
        private final int[] seq;

        private Event(EnumSet<Type> types, int pk, int[] seq)
        {
            this.types = types;
            this.pk = pk;
            this.seq = seq;
        }

        private void process(Observation ob, HistoryValidator.Checker check)
        {
            if (types.contains(Type.READ))
                check.read(pk, ob.id, seq.length, seq);
            if (types.contains(Type.WRITE))
                check.write(pk, ob.id, ob.isSuccess());
        }
    }

    private interface TestDSL
    {
        void txn(Event... events);

        AbstractThrowableAssert<?, ? extends Throwable> failingTxn(Event... events);
    }

    private static boolean supportMultiKey(HistoryValidator validator)
    {
        return validator instanceof StrictSerializabilityValidator;
    }

    private void test(Consumer<TestDSL> fn)
    {
        HistoryValidator validator = create();
        boolean global = supportMultiKey(validator);
        EventIdGen eventIdGen = global ? new AllPks() : new PerPk();
        TestDSL dsl = new TestDSL()
        {
            int logicalClock = 0;

            @Override
            public void txn(Event... events)
            {
                if (global)
                {
                    int eventId = eventIdGen.next();
                    HistoryValidatorTest.txn(validator, ob(eventId, ++logicalClock, ++logicalClock), events);
                }
                else
                {
                    for (Event e : events)
                    {
                        int eventId = eventIdGen.next(e.pk);
                        HistoryValidatorTest.txn(validator, ob(eventId, ++logicalClock, ++logicalClock), e);
                    }
                }
            }

            @Override
            public AbstractThrowableAssert<?, ? extends Throwable> failingTxn(Event... events)
            {
                return assertThatThrownBy(() -> txn(events));
            }
        };
        fn.accept(dsl);
    }

    private interface EventIdGen
    {
        int next(int pk);

        int next();
    }

    private static class PerPk implements EventIdGen
    {
        private final IntIntMap map = new IntIntHashMap();

        @Override
        public int next(int pk)
        {
            int next = !map.containsKey(pk) ? 0 : map.get(pk) + 1;
            map.put(pk, next);
            return next;
        }

        @Override
        public int next()
        {
            throw new UnsupportedOperationException("next without pk not supported");
        }
    }

    private static class AllPks implements EventIdGen
    {
        private int value = 0;

        @Override
        public int next(int pk)
        {
            return next();
        }

        @Override
        public int next()
        {
            return value++;
        }
    }
}
