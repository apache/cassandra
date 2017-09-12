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

package org.apache.cassandra.db.partitions;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;

/**
 * Java 8 version for the partition-locks in {@link AtomicBTreePartition}.
 */
public abstract class AtomicBTreePartitionBase extends AbstractBTreePartition
{
    private static final Logger logger = LoggerFactory.getLogger(AtomicBTreePartitionBase.class);
    private static final Unsafe unsafe;

    static
    {
        logger.info("Initializing Java 8 support for AtomicBTreePartition");

        if (!System.getProperty("java.version").startsWith("1.8.0"))
            throw new RuntimeException("Java 8 required, but running " + System.getProperty("java.version"));

        try
        {
            // Safety... in case someone builds only on Java 8 but runs on Java 11...
            sun.misc.Unsafe.class.getDeclaredMethod("monitorEnter", Object.class);
            sun.misc.Unsafe.class.getDeclaredMethod("monitorExit", Object.class);

            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
        }
        catch (NoSuchFieldException | NoSuchMethodException e)
        {
            throw new RuntimeException("This build of Cassandra has no support for Java 11 as only Java 8 was available during the build. This should never happen.");
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    protected AtomicBTreePartitionBase(DecoratedKey partitionKey)
    {
        super(partitionKey);
    }

    protected final void acquireLock()
    {
        if (unsafe != null)
            unsafe.monitorEnter(this);
    }

    protected final void releaseLock()
    {
        if (unsafe != null)
            unsafe.monitorExit(this);
    }
}
