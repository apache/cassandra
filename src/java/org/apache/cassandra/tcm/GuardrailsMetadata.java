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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.guardrails.CustomGuardrailConfig;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.Transformation.Kind;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.AbstractGuardrailTransformation;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Custom;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Flag;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Thresholds;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Values;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.tcm.Transformation.Kind.CUSTOM;
import static org.apache.cassandra.tcm.Transformation.Kind.FLAG_GUARDRAIL;
import static org.apache.cassandra.tcm.Transformation.Kind.THRESHOLDS_GUARDRAIL;
import static org.apache.cassandra.tcm.Transformation.Kind.VALUES_GUARDRAIL;

public class GuardrailsMetadata implements MetadataValue<GuardrailsMetadata>
{
    public static final GuardrailsMetadata EMPTY = new GuardrailsMetadata(Epoch.EMPTY);
    public static final Serializer serializer = new Serializer();

    private final Epoch lastModified;

    private Map<String, Flag> flags = new HashMap<>();
    private Map<String, Thresholds> thresholds = new HashMap<>();
    private Map<String, Values> values = new HashMap<>();
    private Map<String, Custom> custom = new HashMap<>();

    public GuardrailsMetadata(Epoch lastModified,
                              Map<String, Flag> flags,
                              Map<String, Thresholds> thresholds,
                              Map<String, Values> values,
                              Map<String, Custom> custom)
    {
        this.lastModified = lastModified;
        this.flags = flags;
        this.thresholds = thresholds;
        this.values = values;
        this.custom = custom;
    }

    public GuardrailsMetadata(Epoch lastModified)
    {
        this.lastModified = lastModified;
    }

    @Override
    public GuardrailsMetadata withLastModified(Epoch epoch)
    {
        return new GuardrailsMetadata(epoch, flags, thresholds, values, custom);
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    public Map<String, Flag> getFlags()
    {
        return Collections.unmodifiableMap(flags);
    }

    public Map<String, Thresholds> getThresholds()
    {
        return Collections.unmodifiableMap(thresholds);
    }

    public Map<String, Values> getValues()
    {
        return Collections.unmodifiableMap(values);
    }

    public Map<String, Custom> getCustom()
    {
        return Collections.unmodifiableMap(custom);
    }

    public GuardrailsMetadata with(AbstractGuardrailTransformation t)
    {
        if (t.kind() == FLAG_GUARDRAIL)
            return with((Flag) t);
        else if (t.kind() == VALUES_GUARDRAIL)
            return with((Values) t);
        else if (t.kind() == THRESHOLDS_GUARDRAIL)
            return with((Thresholds) t);
        else if (t.kind() == Kind.CUSTOM_GUARDRAIL)
            return with((Custom) t);
        else
            throw new IllegalArgumentException("Unsupported guardrail type " + t.kind());
    }

    public GuardrailsMetadata with(Flag guardrail)
    {
        Map<String, Flag> newFlags = new HashMap<>(flags);
        newFlags.put(guardrail.name, guardrail);
        return new GuardrailsMetadata(lastModified, ImmutableMap.copyOf(newFlags), thresholds, values, custom);
    }

    public GuardrailsMetadata with(Values guardrail)
    {
        Map<String, Values> newValues = new HashMap<>(values);
        newValues.put(guardrail.name, guardrail);
        return new GuardrailsMetadata(lastModified, flags, thresholds, ImmutableMap.copyOf(newValues), custom);
    }

    public GuardrailsMetadata with(Thresholds guardrail)
    {
        Map<String, Thresholds> newThresholds = new HashMap<>(thresholds);
        newThresholds.put(guardrail.name, guardrail);
        return new GuardrailsMetadata(lastModified, flags, ImmutableMap.copyOf(newThresholds), values, custom);
    }

    public GuardrailsMetadata with(Custom guardrail)
    {
        Map<String, Custom> newCustoms = new HashMap<>(custom);
        newCustoms.put(guardrail.name, guardrail);
        return new GuardrailsMetadata(lastModified, flags, thresholds, values, ImmutableMap.copyOf(newCustoms));
    }

    public ImmutableMap<String, Boolean> diffFlags(GuardrailsMetadata next)
    {
        Map<String, Boolean> diff = new HashMap<>();
        for (Map.Entry<String, Flag> entry : next.flags.entrySet())
        {
            String name = entry.getKey();
            boolean value = entry.getValue().flag;

            Flag oldFlag = flags.get(name);

            if (oldFlag == null || oldFlag.flag != value)
                diff.put(name, value);
        }

        return ImmutableMap.copyOf(diff);
    }

    public ImmutableMap<String, Pair<Long, Long>> diffThresholds(GuardrailsMetadata next)
    {
        Map<String, Pair<Long, Long>> diff = new HashMap<>();

        for (Map.Entry<String, Thresholds> entry : next.thresholds.entrySet())
        {
            String name = entry.getKey();
            Thresholds value = entry.getValue();
            long warnThreshold = value.warnThreshold;
            long failThreshold = value.failThreshold;

            Thresholds oldThreshold = thresholds.get(name);

            if (oldThreshold == null
                || (oldThreshold.failThreshold != failThreshold || oldThreshold.warnThreshold != warnThreshold))
                diff.put(name, Pair.create(value.warnThreshold, value.failThreshold));
        }

        return ImmutableMap.copyOf(diff);
    }

    public ImmutableMap<String, ValuesHolder> diffValues(GuardrailsMetadata next)
    {
        Map<String, ValuesHolder> diff = new HashMap<>();

        for (Map.Entry<String, Values> entry : next.values.entrySet())
        {
            String name = entry.getKey();
            Values oldValues = values.get(name);
            Values newValues = entry.getValue();

            ValuesHolder holder = new ValuesHolder();

            if (oldValues == null)
            {
                holder.disallowed = newValues.disallowed;
                holder.warned = newValues.warned;
                holder.ignored = newValues.ignored;

                if (!(holder.disallowed == null && holder.ignored == null && holder.warned == null))
                    diff.put(name, holder);

                continue;
            }

            if (newValues.disallowed != null && oldValues.disallowed != null)
            {
                if (!newValues.disallowed.equals(oldValues.disallowed))
                    holder.disallowed = ImmutableSet.copyOf(newValues.disallowed);
            }
            else if (newValues.disallowed == null ^ oldValues.disallowed == null)
            {
                if (newValues.disallowed != null && !newValues.disallowed.equals(oldValues.disallowed))
                    holder.disallowed = ImmutableSet.copyOf(newValues.disallowed);
            }

            if (newValues.ignored != null && oldValues.ignored != null)
            {
                if (!newValues.ignored.equals(oldValues.ignored))
                    holder.ignored = ImmutableSet.copyOf(newValues.ignored);
            }
            else if (newValues.ignored == null ^ oldValues.ignored == null)
            {
                if (newValues.ignored != null && !newValues.ignored.equals(oldValues.ignored))
                    holder.ignored = ImmutableSet.copyOf(newValues.ignored);
            }

            if (newValues.warned != null && oldValues.warned != null)
            {
                if (!newValues.warned.equals(oldValues.warned))
                    holder.warned = ImmutableSet.copyOf(newValues.warned);
            }
            else if (newValues.warned == null ^ oldValues.warned == null)
            {
                if (newValues.warned != null && !newValues.warned.equals(oldValues.warned))
                    holder.warned = ImmutableSet.copyOf(newValues.warned);
            }

            if (!(holder.disallowed == null && holder.ignored == null && holder.warned == null))
                diff.put(name, holder);
        }

        return ImmutableMap.copyOf(diff);
    }

    public boolean isDifferent(GuardrailsMetadata next, Kind kind)
    {
        if (kind == FLAG_GUARDRAIL)
            return !diffFlags(next).isEmpty();
        else if (kind == VALUES_GUARDRAIL)
            return !diffValues(next).isEmpty();
        else if (kind == THRESHOLDS_GUARDRAIL)
            return !diffThresholds(next).isEmpty();
        else if (kind == CUSTOM)
            return !diffCustom(next).isEmpty();

        throw new IllegalArgumentException("Unsupported guardrail type " + kind);
    }

    public ImmutableMap<String, CustomGuardrailConfig> diffCustom(GuardrailsMetadata next)
    {
        Map<String, CustomGuardrailConfig> diff = new HashMap<>();

        for (Map.Entry<String, Custom> entry : next.custom.entrySet())
        {
            String name = entry.getKey();
            Custom newCustom = entry.getValue();
            Custom oldCustom = custom.get(name);

            if ((oldCustom != null && newCustom != null && !newCustom.config.equals(oldCustom.config))
                || (oldCustom == null && newCustom != null))
                diff.put(name, newCustom.config);
        }

        return ImmutableMap.copyOf(diff);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GuardrailsMetadata metadata = (GuardrailsMetadata) o;
        return Objects.equals(lastModified, metadata.lastModified)
               && Objects.equals(flags, metadata.flags)
               && Objects.equals(thresholds, metadata.thresholds)
               && Objects.equals(values, metadata.values)
               && Objects.equals(custom, metadata.custom);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lastModified, flags, thresholds, values, custom);
    }

    @Override
    public String toString()
    {
        return "GuardrailsMetadata{" +
               "lastModified=" + lastModified +
               ", flags=" + flags +
               ", thresholds=" + thresholds +
               ", values=" + values +
               ", custom=" + custom +
               '}';
    }

    public static class ValuesHolder
    {
        public Set<String> warned = null;
        public Set<String> ignored = null;
        public Set<String> disallowed = null;

        @Override
        public String toString()
        {
            return "Values{" +
                   "warned=" + warned +
                   ", ignored=" + ignored +
                   ", disallowed=" + disallowed +
                   '}';
        }
    }

    public static class Serializer implements MetadataSerializer<GuardrailsMetadata>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(GuardrailsMetadata t, DataOutputPlus out, Version version) throws IOException
        {
            Epoch.serializer.serialize(t.lastModified, out, version);

            out.writeInt(t.thresholds.size());
            for (Map.Entry<String, Thresholds> entry : t.thresholds.entrySet())
                Thresholds.serializer.serialize(entry.getValue(), out, version);

            out.writeInt(t.values.size());
            for (Map.Entry<String, Values> entry : t.values.entrySet())
                Values.serializer.serialize(entry.getValue(), out, version);

            out.writeInt(t.flags.size());
            for (Map.Entry<String, Flag> entry : t.flags.entrySet())
                Flag.serializer.serialize(entry.getValue(), out, version);

            out.writeInt(t.custom.size());
            for (Map.Entry<String, Custom> entry : t.custom.entrySet())
                Custom.serializer.serialize(entry.getValue(), out, version);
        }

        @Override
        public GuardrailsMetadata deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);

            int thresholdsSize = in.readInt();
            Map<String, Thresholds> thresholdsMap = new HashMap<>(thresholdsSize);
            for (int i = 0; i < thresholdsSize; i++)
            {
                Thresholds thresholds = Thresholds.serializer.deserialize(in, version);
                thresholdsMap.put(thresholds.name, thresholds);
            }

            int valuesSize = in.readInt();
            Map<String, Values> valuesMap = new HashMap<>(valuesSize);
            for (int i = 0; i < valuesSize; i++)
            {
                Values values = Values.serializer.deserialize(in, version);
                valuesMap.put(values.name, values);
            }

            int flagsSize = in.readInt();
            Map<String, Flag> flagsMap = new HashMap<>(flagsSize);
            for (int i = 0; i < flagsSize; i++)
            {
                Flag flag = Flag.serializer.deserialize(in, version);
                flagsMap.put(flag.name, flag);
            }

            int customSize = in.readInt();
            Map<String, Custom> customMap = new HashMap<>(customSize);
            for (int i = 0; i < customSize; i++)
            {
                Custom custom = Custom.serializer.deserialize(in, version);
                customMap.put(custom.name, custom);
            }

            return new GuardrailsMetadata(lastModified,
                                          ImmutableMap.copyOf(flagsMap),
                                          ImmutableMap.copyOf(thresholdsMap),
                                          ImmutableMap.copyOf(valuesMap),
                                          ImmutableMap.copyOf(customMap));
        }

        @Override
        public long serializedSize(GuardrailsMetadata t, Version version)
        {
            long epochSize = Epoch.serializer.serializedSize(t.lastModified, version);

            long thresholdsSize = 0;
            thresholdsSize += TypeSizes.INT_SIZE;
            for (Map.Entry<String, Thresholds> entry : t.thresholds.entrySet())
                thresholdsSize += Thresholds.serializer.serializedSize(entry.getValue(), version);

            long valuesSize = 0;
            valuesSize += TypeSizes.INT_SIZE;
            for (Map.Entry<String, Values> entry : t.values.entrySet())
                valuesSize += Values.serializer.serializedSize(entry.getValue(), version);

            long flagsSize = 0;
            flagsSize += TypeSizes.INT_SIZE;
            for (Map.Entry<String, Flag> entry : t.flags.entrySet())
                flagsSize += Flag.serializer.serializedSize(entry.getValue(), version);

            long customSize = 0;
            customSize += TypeSizes.INT_SIZE;
            for (Map.Entry<String, Custom> entry : t.custom.entrySet())
                customSize += Custom.serializer.serializedSize(entry.getValue(), version);

            return epochSize + thresholdsSize + valuesSize + flagsSize + customSize;
        }
    }
}
