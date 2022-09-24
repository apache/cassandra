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

package org.apache.cassandra.tcm.transformations;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.guardrails.CustomGuardrailConfig;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.GuardrailsMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.tcm.sequences.LockedRanges.AffectedRanges.EMPTY;

public class GuardrailTransformations
{
    private GuardrailTransformations()
    {
    }

    public static class Flag extends AbstractGuardrailTransformation
    {
        public static final Serializer serializer = new Serializer();

        public final boolean flag;

        public Flag(String name, boolean flag)
        {
            super(name, Kind.FLAG_GUARDRAIL);
            this.flag = flag;
        }

        @Override
        public String toString()
        {
            return "Flag{" +
                   "name='" + name + '\'' +
                   ", flag=" + flag +
                   '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Flag other = (Flag) o;
            return flag == other.flag && Objects.equals(name, other.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, flag);
        }

        public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, Flag>
        {
            private Serializer()
            {
            }

            @Override
            public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
            {
                Flag flag = (Flag) t;
                out.writeUTF(flag.name);
                out.writeBoolean(flag.flag);
            }

            @Override
            public Flag deserialize(DataInputPlus in, Version version) throws IOException
            {
                String name = in.readUTF();
                boolean flag = in.readBoolean();
                return new Flag(name, flag);
            }

            @Override
            public long serializedSize(Transformation t, Version version)
            {
                Flag flag = (Flag) t;
                return (long) TypeSizes.sizeof(flag.name) + TypeSizes.sizeof(flag.flag);
            }
        }
    }

    public static class Values extends AbstractGuardrailTransformation
    {
        public static final Serializer serializer = new Serializer();

        public final Set<String> warned;
        public final Set<String> disallowed;
        public final Set<String> ignored;

        public Values(String name, Set<String> warned, Set<String> disallowed, Set<String> ignored)
        {
            super(name, Kind.VALUES_GUARDRAIL);
            this.warned = warned;
            this.disallowed = disallowed;
            this.ignored = ignored;
        }

        @Override
        public String toString()
        {
            return "Values{" +
                   "name='" + name + '\'' +
                   ", warned=" + warned +
                   ", disallowed=" + disallowed +
                   ", ignored=" + ignored +
                   '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Values other = (Values) o;
            return Objects.equals(name, other.name)
                   && Objects.equals(warned, other.warned)
                   && Objects.equals(disallowed, other.disallowed)
                   && Objects.equals(ignored, other.ignored);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, warned, disallowed, ignored);
        }

        public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, Values>
        {
            private Serializer()
            {
            }

            @Override
            public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
            {
                Values values = (Values) t;
                out.writeUTF(values.name);

                int ignoredSize = values.ignored == null ? -1 : values.ignored.size();
                out.writeInt(ignoredSize);
                if (ignoredSize > 0)
                    for (String ignored : values.ignored)
                        out.writeUTF(ignored);

                int warnedSize = values.warned == null ? -1 : values.warned.size();
                out.writeInt(warnedSize);
                if (warnedSize > 0)
                    for (String warned : values.warned)
                        out.writeUTF(warned);

                int disallowedSize = values.disallowed == null ? -1 : values.disallowed.size();
                out.writeInt(disallowedSize);
                if (disallowedSize > 0)
                    for (String disallowed : values.disallowed)
                        out.writeUTF(disallowed);
            }

            @Override
            public Values deserialize(DataInputPlus in, Version version) throws IOException
            {
                String name = in.readUTF();

                Set<String> ignored = null;
                Set<String> warned = null;
                Set<String> disallowed = null;

                int ignoredSize = in.readInt();
                if (ignoredSize > -1)
                {
                    ignored = new HashSet<>();
                    for (int i = 0; i < ignoredSize; i++)
                        ignored.add(in.readUTF());
                }

                int warnedSize = in.readInt();
                if (warnedSize > -1)
                {
                    warned = new HashSet<>();
                    for (int i = 0; i < warnedSize; i++)
                        warned.add(in.readUTF());
                }

                int disallowedSize = in.readInt();
                if (disallowedSize > -1)
                {
                    disallowed = new HashSet<>();
                    for (int i = 0; i < disallowedSize; i++)
                        disallowed.add(in.readUTF());
                }

                return new Values(name, warned, disallowed, ignored);
            }

            @Override
            public long serializedSize(Transformation t, Version version)
            {
                Values values = (Values) t;
                long entriesSizes = 0;
                if (values.ignored != null)
                    for (String s : values.ignored)
                        entriesSizes += TypeSizes.sizeof(s);

                if (values.warned != null)
                    for (String s : values.warned)
                        entriesSizes += TypeSizes.sizeof(s);

                if (values.disallowed != null)
                    for (String s : values.disallowed)
                        entriesSizes += TypeSizes.sizeof(s);

                return (long) TypeSizes.sizeof(values.name) + (3 * TypeSizes.INT_SIZE) + entriesSizes;
            }
        }
    }

    public static class Thresholds extends AbstractGuardrailTransformation
    {
        public static final Serializer serializer = new Serializer();

        public final long warnThreshold;
        public final long failThreshold;

        public Thresholds(String name, long warnThreshold, long failThreshold)
        {
            super(name, Kind.THRESHOLDS_GUARDRAIL);
            this.warnThreshold = warnThreshold;
            this.failThreshold = failThreshold;
        }

        @Override
        public String toString()
        {
            return "Thresholds{" +
                   "name='" + name + '\'' +
                   ", warnThreshold=" + warnThreshold +
                   ", failThreshold=" + failThreshold +
                   '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Thresholds other = (Thresholds) o;
            return name.equals(other.name) && warnThreshold == other.warnThreshold && failThreshold == other.failThreshold;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, warnThreshold, failThreshold);
        }

        public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, Thresholds>
        {
            private Serializer()
            {
            }

            @Override
            public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
            {
                Thresholds thresholds = (Thresholds) t;
                out.writeUTF(thresholds.name);
                out.writeLong(thresholds.warnThreshold);
                out.writeLong(thresholds.failThreshold);
            }

            @Override
            public Thresholds deserialize(DataInputPlus in, Version version) throws IOException
            {
                String name = in.readUTF();
                long warnThreshold = in.readLong();
                long failThreshold = in.readLong();
                return new Thresholds(name, warnThreshold, failThreshold);
            }

            @Override
            public long serializedSize(Transformation t, Version version)
            {
                Thresholds thresholds = (Thresholds) t;
                return (long) TypeSizes.sizeof(thresholds.warnThreshold)
                       + TypeSizes.sizeof(thresholds.failThreshold)
                       + TypeSizes.sizeof(thresholds.name);
            }
        }
    }

    public static class Custom extends AbstractGuardrailTransformation
    {
        public static final Serializer serializer = new Serializer();

        public final CustomGuardrailConfig config;

        public Custom(String name, CustomGuardrailConfig config)
        {
            super(name, Kind.CUSTOM_GUARDRAIL);
            this.config = config;
        }

        @Override
        public String toString()
        {
            return "Custom{" +
                   "name='" + name + '\'' +
                   ", config=" + config +
                   '}';
        }

        public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, Custom>
        {
            private Serializer()
            {
            }

            @Override
            public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
            {
                Custom custom = (Custom) t;
                out.writeUTF(custom.name);
                out.writeUTF(JsonUtils.writeAsJsonString(custom.config));
            }

            @Override
            public Custom deserialize(DataInputPlus in, Version version) throws IOException
            {
                String name = in.readUTF();
                String configString = in.readUTF();
                return new Custom(name, new CustomGuardrailConfig(JsonUtils.fromJsonMap(configString)));
            }

            @Override
            public long serializedSize(Transformation t, Version version)
            {
                Custom custom = (Custom) t;
                return (long) TypeSizes.sizeof(custom.name) + TypeSizes.sizeof(JsonUtils.writeAsJsonString(custom.config));
            }
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Custom other = (Custom) o;
            return name.equals(other.name) && Objects.equals(config, other.config);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, config);
        }
    }

    public abstract static class AbstractGuardrailTransformation implements Transformation
    {
        public final String name;
        private final Kind kind;

        AbstractGuardrailTransformation(String name, Kind kind)
        {
            this.name = name;
            this.kind = kind;
        }

        @Override
        public Kind kind()
        {
            return kind;
        }

        @Override
        public Result execute(ClusterMetadata metadata)
        {
            GuardrailsMetadata with = metadata.guardrailsMetadata.with(this);
            if (metadata.guardrailsMetadata.isDifferent(with, kind()))
                return Transformation.success(metadata.transformer().with(this), EMPTY);
            else
                return new Transformation.Rejected(ExceptionCode.INVALID, "idempotent application");
        }
    }
}
