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

package org.apache.cassandra.service.epaxos;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.Pair;

/**
 * Since execution of instances is suspended during some stream tasks,
 * this works it's way through a range and executes any instances that
 * would have been skipped
 */
public abstract class PostStreamTask implements Runnable
{
    protected final EpaxosService service;
    protected final UUID cfId;

    protected PostStreamTask(EpaxosService service, UUID cfId)
    {
        this.service = service;
        this.cfId = cfId;
    }

    Comparator<UUID> comparator = new Comparator<UUID>()
    {
        @Override
        public int compare(UUID o1, UUID o2)
        {
            return DependencyGraph.comparator.compare(o2, o1);
        }
    };

    protected abstract Iterator<KeyState> getIterator();

    @Override
    public void run()
    {
        Iterator<KeyState> iter = getIterator();
        while (iter.hasNext())
        {
            KeyState ks = iter.next();
            List<UUID> activeIds = new ArrayList<>(ks.getActiveInstanceIds());

            // sort ids so most recently created ones will be at the head of the list
            // even though the first committed instance may not be the first to execute,
            // it will be in the first strongly connected component
            Collections.sort(activeIds, comparator);
            for (UUID id: activeIds)
            {
                Instance instance = service.loadInstance(id);
                if (instance != null)
                {
                    if (instance.getState().atLeast(Instance.State.EXECUTED))
                    {
                        // nothing to do here
                        break;
                    }
                    else if (instance.getState().atLeast(Instance.State.COMMITTED))
                    {
                        service.execute(id);
                        break;
                    }
                }
            }
        }
    }

    public static class Ranged extends PostStreamTask
    {
        private final Range<Token> range;
        private final Scope scope;

        public Ranged(EpaxosService service, UUID cfId, Range<Token> range, Scope scope)
        {
            super(service, cfId);
            this.range = range;
            this.scope = scope;
        }

        @Override
        protected Iterator<KeyState> getIterator()
        {
            return service.getKeyStateManager(scope).getStaleKeyStateRange(cfId, range);
        }
    }

    public static class KeyCollection extends PostStreamTask
    {
        private final Collection<Pair<ByteBuffer, Scope>> keys;

        public KeyCollection(EpaxosService service, UUID cfId, Collection<Pair<ByteBuffer, Scope>> keys)
        {
            super(service, cfId);
            this.keys = keys;
        }

        @Override
        protected Iterator<KeyState> getIterator()
        {
            final Iterator<Pair<ByteBuffer, Scope>> iter = keys.iterator();
            return new AbstractIterator<KeyState>()
            {
                @Override
                protected KeyState computeNext()
                {
                    if (!iter.hasNext())
                    {
                        return endOfData();
                    }
                    Pair<ByteBuffer, Scope> next = iter.next();
                    return service.getKeyStateManager(next.right).loadKeyState(next.left, cfId);
                }
            };
        }
    }
}
