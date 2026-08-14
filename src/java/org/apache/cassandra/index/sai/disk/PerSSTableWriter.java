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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;

import com.google.common.base.Stopwatch;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * Writes all SSTable-attached index token and offset structures.
 */
public interface PerSSTableWriter
{
    PerSSTableWriter NONE = key -> {};

    /**
     * Allows implementations to perform any necessary setup for a new partition.
     *
     * @param decoratedKey The key being appended to SSTable.
     * @param position     The position of the key in the component preferred for reading keys
     */
    default void startPartition(DecoratedKey decoratedKey, long position) throws IOException
    {}

    void nextRow(PrimaryKey primaryKey) throws IOException;

    default void complete(Stopwatch stopwatch) throws IOException
    {}

    default void abort(Throwable accumulator)
    {}
}
