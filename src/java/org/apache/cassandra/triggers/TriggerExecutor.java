/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.triggers;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.cassandra.config.TriggerDefinition;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class TriggerExecutor
{
    public static final TriggerExecutor instance = new TriggerExecutor();

    private final Map<String, ITrigger> cachedTriggers = Maps.newConcurrentMap();
    private final ClassLoader parent = Thread.currentThread().getContextClassLoader();
    private volatile ClassLoader customClassLoader;

    private TriggerExecutor()
    {
        reloadClasses();
    }

    /**
     * Reload the triggers which is already loaded, Invoking this will update
     * the class loader so new jars can be loaded.
     */
    public void reloadClasses()
    {
        File tiggerDirectory = FBUtilities.cassandraTriggerDir();
        if (tiggerDirectory == null)
            return;
        customClassLoader = new CustomClassLoader(parent, tiggerDirectory);
        cachedTriggers.clear();
    }

    public ColumnFamily execute(ByteBuffer key, ColumnFamily updates) throws InvalidRequestException
    {
        List<Mutation> intermediate = executeInternal(key, updates);
        if (intermediate == null || intermediate.isEmpty())
            return updates;

        validateForSinglePartition(updates.metadata().getKeyValidator(), updates.id(), key, intermediate);

        for (Mutation mutation : intermediate)
        {
            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                updates.addAll(cf);
            }
        }
        return updates;
    }

    public Collection<Mutation> execute(Collection<? extends IMutation> mutations) throws InvalidRequestException
    {
        boolean hasCounters = false;
        List<Mutation> augmentedMutations = null;

        for (IMutation mutation : mutations)
        {
            if (mutation instanceof CounterMutation)
                hasCounters = true;

            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                List<Mutation> augmentations = executeInternal(mutation.key(), cf);
                if (augmentations == null || augmentations.isEmpty())
                    continue;

                validate(augmentations);

                if (augmentedMutations == null)
                    augmentedMutations = new LinkedList<>();
                augmentedMutations.addAll(augmentations);
            }
        }

        if (augmentedMutations == null)
            return null;

        if (hasCounters)
            throw new InvalidRequestException("Counter mutations and trigger mutations cannot be applied together atomically.");

        @SuppressWarnings("unchecked")
        Collection<Mutation> originalMutations = (Collection<Mutation>) mutations;

        return mergeMutations(Iterables.concat(originalMutations, augmentedMutations));
    }

    private Collection<Mutation> mergeMutations(Iterable<Mutation> mutations)
    {
        Map<Pair<String, ByteBuffer>, Mutation> groupedMutations = new HashMap<>();

        for (Mutation mutation : mutations)
        {
            Pair<String, ByteBuffer> key = Pair.create(mutation.getKeyspaceName(), mutation.key());
            Mutation current = groupedMutations.get(key);
            if (current == null)
            {
                // copy in case the mutation's modifications map is backed by an immutable Collections#singletonMap().
                groupedMutations.put(key, mutation.copy());
            }
            else
            {
                current.addAll(mutation);
            }
        }

        return groupedMutations.values();
    }

    private void validateForSinglePartition(AbstractType<?> keyValidator,
                                            UUID cfId,
                                            ByteBuffer key,
                                            Collection<Mutation> tmutations)
    throws InvalidRequestException
    {
        for (Mutation mutation : tmutations)
        {
            if (keyValidator.compare(mutation.key(), key) != 0)
                throw new InvalidRequestException("Partition key of additional mutation does not match primary update key");

            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                if (! cf.id().equals(cfId))
                    throw new InvalidRequestException("table of additional mutation does not match primary update table");
            }
        }
        validate(tmutations);
    }

    private void validate(Collection<Mutation> tmutations) throws InvalidRequestException
    {
        for (Mutation mutation : tmutations)
        {
            QueryProcessor.validateKey(mutation.key());
            for (ColumnFamily tcf : mutation.getColumnFamilies())
                for (Cell cell : tcf)
                    cell.validateFields(tcf.metadata());
        }
    }

    /**
     * Switch class loader before using the triggers for the column family, if
     * not loaded them with the custom class loader.
     */
    private List<Mutation> executeInternal(ByteBuffer key, ColumnFamily columnFamily)
    {
        Map<String, TriggerDefinition> triggers = columnFamily.metadata().getTriggers();
        if (triggers.isEmpty())
            return null;
        List<Mutation> tmutations = Lists.newLinkedList();
        Thread.currentThread().setContextClassLoader(customClassLoader);
        try
        {
            for (TriggerDefinition td : triggers.values())
            {
                ITrigger trigger = cachedTriggers.get(td.classOption);
                if (trigger == null)
                {
                    trigger = loadTriggerInstance(td.classOption);
                    cachedTriggers.put(td.classOption, trigger);
                }
                Collection<Mutation> temp = trigger.augment(key, columnFamily);
                if (temp != null)
                    tmutations.addAll(temp);
            }
            return tmutations;
        }
        catch (Exception ex)
        {
            throw new RuntimeException(String.format("Exception while creating trigger on table with ID: %s", columnFamily.id()), ex);
        }
        finally
        {
            Thread.currentThread().setContextClassLoader(parent);
        }
    }

    public synchronized ITrigger loadTriggerInstance(String triggerName) throws Exception
    {
        // double check.
        if (cachedTriggers.get(triggerName) != null)
            return cachedTriggers.get(triggerName);
        return (ITrigger) customClassLoader.loadClass(triggerName).getConstructor().newInstance();
    }
}
