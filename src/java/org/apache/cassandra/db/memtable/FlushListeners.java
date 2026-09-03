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

package org.apache.cassandra.db.memtable;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.schema.TableMetadata;

/**
 * The callbacks to run once a memtable generation is flushed to disk
 */
class FlushListeners
{
    private Map<Object, Consumer<TableMetadata>> onFlush = ImmutableMap.of();

    @SuppressWarnings("unchecked")
    synchronized <T extends Consumer<TableMetadata>> T ensureFlushListener(Object key, Supplier<T> factory)
    {
        if (onFlush == null)
            return null;

        T listener = (T) onFlush.get(key);
        if (null == listener)
        {
            listener = factory.get();
            onFlush = ImmutableMap.<Object, Consumer<TableMetadata>>builder()
                                  .putAll(onFlush)
                                  .put(key, listener)
                                  .build();
        }
        return listener;
    }

    void notifyFlushed(TableMetadata metadata)
    {
        Collection<Consumer<TableMetadata>> run;
        synchronized (this)
        {
            run = onFlush.values();
            onFlush = null;
        }
        run.forEach(c -> c.accept(metadata));
    }
}
