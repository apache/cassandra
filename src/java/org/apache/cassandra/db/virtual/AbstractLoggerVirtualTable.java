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

package org.apache.cassandra.db.virtual;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.spi.LoggingEvent;
import org.apache.cassandra.schema.TableMetadata;

/**
 * This table is inherently limited on number of rows it can hold.
 *
 * @param <U> type parameter saying what object is stored in internal bounded list for query purposes
 */
public abstract class AbstractLoggerVirtualTable<U> extends AbstractMutableVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractLoggerVirtualTable.class);

    // please be sure operations on this structure are thread-safe
    protected final List<U> buffer;

    @VisibleForTesting
    protected static int resolveBufferSize(int wantedSize, int max, int defaultSize)
    {
        return (wantedSize < 1 || wantedSize > max) ? defaultSize : wantedSize;
    }

    protected AbstractLoggerVirtualTable(TableMetadata metadata, int maxSize)
    {
        super(metadata);
        this.buffer = BoundedLinkedList.create(maxSize);
        logger.debug("capacity of virtual table {} is set to be at most {} rows", metadata().toString(), maxSize);
    }

    public void add(LoggingEvent event)
    {
        List<U> messages = getMessages(event);
        if (messages != null)
        {
            // specifically calling buffer.add to reach BoundedLinkedList's add
            // instead of linked list's addAll
            for (U message : messages)
                buffer.add(message);
        }
    }

    public abstract List<U> getMessages(LoggingEvent event);

    @Override
    public void truncate()
    {
        synchronized (buffer)
        {
            buffer.clear();
        }
    }

    @Override
    public boolean allowFilteringImplicitly()
    {
        return false;
    }

    private static final class BoundedLinkedList<T> extends LinkedList<T>
    {
        private final int maxSize;

        public static <T> List<T> create(int size)
        {
            return Collections.synchronizedList(new BoundedLinkedList<>(size));
        }

        private BoundedLinkedList(int maxSize)
        {
            this.maxSize = maxSize;
        }

        @Override
        public synchronized boolean add(T t)
        {
            if (size() == maxSize)
                removeLast();

            addFirst(t);

            return true;
        }
    }
}
