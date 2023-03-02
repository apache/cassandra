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
import java.util.*;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.extensions.IntValue;
import org.apache.cassandra.tcm.extensions.StringValue;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.sequences.LockedRanges;

public class CustomTransformation implements Transformation
{
    public static Serializer serializer = new Serializer();
    private final String extension;
    private final Transformation child;

    private static Map<String, AsymmetricMetadataSerializer<Transformation, ? extends Transformation>> extensions = new HashMap<>();

    static
    {
        registerExtension(PokeString.NAME, new PokeString.TransformSerializer());
        registerExtension(PokeInt.NAME, new PokeInt.TransformSerializer());
    }

    public CustomTransformation(String extension, Transformation child)
    {
        this.extension = extension;
        this.child = child;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomTransformation that = (CustomTransformation) o;
        return Objects.equals(child, that.child);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(child);
    }

    public Transformation child()
    {
        return child;
    }

    public static CustomTransformation make(String v)
    {
        return new CustomTransformation(PokeString.NAME, new PokeString(v));
    }

    public static CustomTransformation make(int v)
    {
        return new CustomTransformation(PokeInt.NAME, new PokeInt(v));
    }

    public static AsymmetricMetadataSerializer<Transformation, ? extends Transformation> getSerializer(String name)
    {
        assert extensions.containsKey(name) : String.format("Unknown extension %s. Available extensions %s.", name, extensions.keySet());
        return extensions.get(name);
    }

    public static void registerExtension(String name, AsymmetricMetadataSerializer<Transformation, ? extends Transformation> callback)
    {
        extensions.put(name, callback);
    }

    public static void unregisterExtension(String name)
    {
        extensions.remove(name);
    }

    public static void clearExtensions()
    {
        extensions.clear();
    }

    public Kind kind()
    {
        return Kind.CUSTOM;
    }

    public Result execute(ClusterMetadata prev)
    {
        return child.execute(prev);
    }

    public String toString()
    {
        return "CustomTransformation" +
               "{" +
               "extension='" + extension + '\'' +
               ", child=" + child +
               '}';
    }

    public static class Serializer implements AsymmetricMetadataSerializer<Transformation, Transformation>
    {
        private Serializer() {}

        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            CustomTransformation s = (CustomTransformation) t;
            out.writeInt(s.extension.length());
            out.writeBytes(s.extension);
            extensions.get(s.extension).serialize(s.child, out, version);
        }

        public CustomTransformation deserialize(DataInputPlus in, Version version) throws IOException
        {
            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            String extension = new String(bytes);
            return new CustomTransformation(extension, getSerializer(extension).deserialize(in, version));
        }

        public long serializedSize(Transformation t, Version version)
        {
            CustomTransformation s = (CustomTransformation) t;
            return Integer.BYTES + s.extension.length() + getSerializer(s.extension).serializedSize(s.child, version);
        }
    }

    public static class PokeString implements Transformation
    {
        // how to identify and obtain a serializer for instances of this Transformation
        public static final String NAME = PokeString.class.getName();

        // This Transformation will insert/overwrite a value in the map ClusterMetadata maintains for arbitrary values.
        // To do this, it must provide a key to identify the value and a wrapper type for serialization.
        public static final ExtensionKey<String, StringValue> METADATA_KEY = new ExtensionKey<>(NAME, StringValue.class);

        public final String str;

        public PokeString(String str)
        {
            this.str = str;
        }

        public Kind kind()
        {
            return Kind.CUSTOM;
        }

        public Result execute(ClusterMetadata prev)
        {
            StringValue value = StringValue.create(str);
            return success(prev.transformer().with(METADATA_KEY, value), LockedRanges.AffectedRanges.EMPTY);
        }

        public String toString()
        {
            return String.format("String{'%s'}", str);
        }

        public static class TransformSerializer implements AsymmetricMetadataSerializer<Transformation, PokeString>
        {
            private TransformSerializer() {}

            public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
            {
                PokeString s = (PokeString) t;
                out.writeInt(s.str.length());
                out.writeBytes(s.str);
            }

            public PokeString deserialize(DataInputPlus in, Version version) throws IOException
            {
                int size = in.readInt();
                byte[] bytes = new byte[size];
                in.readFully(bytes);
                return new PokeString(new String(bytes));
            }

            public long serializedSize(Transformation t, Version version)
            {
                return Integer.BYTES + ((PokeString) t).str.length();
            }
        }
    }

    public static class PokeInt implements Transformation
    {
        // how to identify and obtain a serializer for instances of this Transformation
        public static final String NAME = PokeInt.class.getName();

        // This Transformation will insert/overwrite a value in the map ClusterMetadata maintains for arbitrary values.
        // To do this, it must provide a key to identify the value and a wrapper type for serialization.
        public static final ExtensionKey<Integer, IntValue> METADATA_KEY = new ExtensionKey<>(NAME, IntValue.class);

        public final int v;

        public PokeInt(int v)
        {
            this.v = v;
        }

        public Kind kind()
        {
            return Kind.CUSTOM;
        }

        public Result execute(ClusterMetadata prev)
        {
            IntValue value = IntValue.create(v);
            return success(prev.transformer().with(METADATA_KEY, value), LockedRanges.AffectedRanges.EMPTY);
        }

        public String toString()
        {
            return String.format("int{%d}", v);
        }

        public static class TransformSerializer implements AsymmetricMetadataSerializer<Transformation, PokeInt>
        {
            private TransformSerializer() {}

            public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
            {
                PokeInt s = (PokeInt) t;
                out.writeInt(s.v);
            }

            public PokeInt deserialize(DataInputPlus in, Version version) throws IOException
            {
                int v = in.readInt();
                return new PokeInt(v);
            }

            public long serializedSize(Transformation t, Version version)
            {
                return Integer.BYTES;
            }
        }
    }
}