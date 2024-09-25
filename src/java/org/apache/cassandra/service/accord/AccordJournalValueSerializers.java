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

package org.apache.cassandra.service.accord;

import java.io.IOException;

import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.service.accord.AccordKeyspace.LocalVersionedSerializers.durableBefore;
import static org.apache.cassandra.service.accord.AccordKeyspace.LocalVersionedSerializers.redundantBefore;

public class AccordJournalValueSerializers
{
    public interface FlyweightSerializer<WRITE, BUILDER>
    {
        BUILDER mergerFor(JournalKey key);
        void serialize(JournalKey key, WRITE from, DataOutputPlus out, int userVersion) throws IOException;
        void deserialize(JournalKey key, BUILDER into, DataInputPlus in, int userVersion) throws IOException;
    }

    public static class CommandDiffSerializer implements FlyweightSerializer<SavedCommand.DiffWriter, SavedCommand.Builder>
    {
        @Override
        public SavedCommand.Builder mergerFor(JournalKey journalKey)
        {
            return new SavedCommand.Builder();
        }

        @Override
        public void serialize(JournalKey key, SavedCommand.DiffWriter writer, DataOutputPlus out, int userVersion)
        {
            try
            {
                writer.write(out, userVersion);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void deserialize(JournalKey journalKey, SavedCommand.Builder into, DataInputPlus in, int userVersion) throws IOException
        {
            into.deserializeNext(in, userVersion);
        }
    }

    public abstract static class Accumulator<A, V>
    {
        private A accumulated;

        public Accumulator(A initial)
        {
            this.accumulated = initial;
        }

        protected void update(V newValue)
        {
            accumulated = accumulate(accumulated, newValue);
        }

        protected abstract A accumulate(A oldValue, V newValue);

        public A get()
        {
            return accumulated;
        }
    }

    public static class RedundantBeforeAccumulator extends Accumulator<RedundantBefore, RedundantBefore>
    {
        public RedundantBeforeAccumulator()
        {
            super(RedundantBefore.EMPTY);
        }

        @Override
        protected RedundantBefore accumulate(RedundantBefore oldValue, RedundantBefore newValue)
        {
            return RedundantBefore.merge(oldValue, newValue);
        }
    }

    public static class DurableBeforeAccumulator extends Accumulator<DurableBefore, DurableBefore>
    {
        public DurableBeforeAccumulator()
        {
            super(DurableBefore.EMPTY);
        }

        @Override
        protected DurableBefore accumulate(DurableBefore oldValue, DurableBefore newValue)
        {
            return DurableBefore.merge(oldValue, newValue);
        }
    }

    public static class RedundantBeforeSerializer implements FlyweightSerializer<RedundantBefore, RedundantBeforeAccumulator>
    {
        @Override
        public RedundantBeforeAccumulator mergerFor(JournalKey journalKey)
        {
            return new RedundantBeforeAccumulator();
        }

        @Override
        public void serialize(JournalKey key, RedundantBefore entry, DataOutputPlus out, int userVersion)
        {
            try
            {
                if (entry == RedundantBefore.EMPTY)
                {
                    out.writeInt(0);
                    return;
                }
                out.writeInt(1);
                redundantBefore.serialize(entry, out);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void deserialize(JournalKey journalKey, RedundantBeforeAccumulator into, DataInputPlus in, int userVersion) throws IOException
        {
            if (in.readInt() == 0)
            {
                into.update(RedundantBefore.EMPTY);
                return;
            }
            // TODO: maybe using local serializer is not the best call here, but how do we distinguish
            // between messaging and disk versioning?
            into.update(redundantBefore.deserialize(in));
        }
    }

    public static class DurableBeforeSerializer implements FlyweightSerializer<DurableBefore, DurableBeforeAccumulator>
    {
        public DurableBeforeAccumulator mergerFor(JournalKey journalKey)
        {
            return new DurableBeforeAccumulator();
        }

        @Override
        public void serialize(JournalKey key, DurableBefore entry, DataOutputPlus out, int userVersion)
        {
            try
            {
                durableBefore.serialize(entry, out);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void deserialize(JournalKey journalKey, DurableBeforeAccumulator into, DataInputPlus in, int userVersion) throws IOException
        {
            // TODO: maybe using local serializer is not the best call here, but how do we distinguish
            // between messaging and disk versioning?
            into.update(durableBefore.deserialize(in));
        }
    }
}
