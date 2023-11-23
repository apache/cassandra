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
package org.apache.cassandra.schema;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static com.google.common.collect.Iterables.filter;
import static org.apache.cassandra.db.TypeSizes.sizeof;

public final class Triggers implements Iterable<TriggerMetadata>
{
    public static final Serializer serializer = new Serializer();

    private final ImmutableMap<String, TriggerMetadata> triggers;

    private Triggers(Builder builder)
    {
        triggers = builder.triggers.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Triggers none()
    {
        return builder().build();
    }

    public static Triggers of(TriggerMetadata... triggers)
    {
        return builder().add(triggers).build();
    }

    public static Triggers of(Iterable<TriggerMetadata> triggers)
    {
        return builder().add(triggers).build();
    }

    public Iterator<TriggerMetadata> iterator()
    {
        return triggers.values().iterator();
    }

    public int size()
    {
        return triggers.size();
    }

    public boolean isEmpty()
    {
        return triggers.isEmpty();
    }

    /**
     * Get the trigger with the specified name
     *
     * @param name a non-qualified trigger name
     * @return an empty {@link Optional} if the trigger name is not found; a non-empty optional of {@link TriggerMetadata} otherwise
     */
    public Optional<TriggerMetadata> get(String name)
    {
        return Optional.ofNullable(triggers.get(name));
    }

    /**
     * Create a Triggers instance with the provided trigger added
     */
    public Triggers with(TriggerMetadata trigger)
    {
        if (get(trigger.name).isPresent())
            throw new IllegalStateException(String.format("Trigger %s already exists", trigger.name));

        return builder().add(this).add(trigger).build();
    }

    /**
     * Creates a Triggers instance with the trigger with the provided name removed
     */
    public Triggers without(String name)
    {
        TriggerMetadata trigger =
            get(name).orElseThrow(() -> new IllegalStateException(String.format("Trigger %s doesn't exists", name)));

        return builder().add(filter(this, t -> t != trigger)).build();
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof Triggers && triggers.equals(((Triggers) o).triggers));
    }

    @Override
    public int hashCode()
    {
        return triggers.hashCode();
    }

    @Override
    public String toString()
    {
        return triggers.values().toString();
    }

    public static final class Builder
    {
        final ImmutableMap.Builder<String, TriggerMetadata> triggers = new ImmutableMap.Builder<>();

        private Builder()
        {
        }

        public Triggers build()
        {
            return new Triggers(this);
        }

        public Builder add(TriggerMetadata trigger)
        {
            triggers.put(trigger.name, trigger);
            return this;
        }

        public Builder add(TriggerMetadata... triggers)
        {
            for (TriggerMetadata trigger : triggers)
                add(trigger);
            return this;
        }

        public Builder add(Iterable<TriggerMetadata> triggers)
        {
            triggers.forEach(this::add);
            return this;
        }
    }

    public static class Serializer implements MetadataSerializer<Triggers>
    {
        public void serialize(Triggers t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeInt(t.triggers.size());
            for (TriggerMetadata tm : t.triggers.values())
                TriggerMetadata.serializer.serialize(tm, out, version);
        }

        public Triggers deserialize(DataInputPlus in, Version version) throws IOException
        {
            int size = in.readInt();
            Builder builder = builder();
            for (int i = 0; i < size; i++)
            {
                TriggerMetadata tm = TriggerMetadata.serializer.deserialize(in, version);
                builder.add(tm);
            }
            return builder.build();
        }

        public long serializedSize(Triggers t, Version version)
        {
            int size = sizeof(t.triggers.size());
            for (TriggerMetadata tm : t.triggers.values())
                size += TriggerMetadata.serializer.serializedSize(tm, version);
            return size;
        }
    }
}
