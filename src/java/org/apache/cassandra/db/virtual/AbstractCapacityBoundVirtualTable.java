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

import org.apache.cassandra.schema.TableMetadata;

public abstract class AbstractCapacityBoundVirtualTable<U> extends AbstractMutableVirtualTable
{
    protected AbstractCapacityBoundVirtualTable(TableMetadata metadata)
    {
        super(metadata);
    }

    @VisibleForTesting
    protected static int resolveBufferSize(int wantedSize, int max, int defaultSize)
    {
        return (wantedSize < 1 || wantedSize > max) ? defaultSize : wantedSize;
    }

    @Override
    public boolean allowFilteringImplicitly()
    {
        return false;
    }

    static final class BoundedLinkedList<T> extends LinkedList<T>
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
