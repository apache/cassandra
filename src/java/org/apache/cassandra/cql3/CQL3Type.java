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
package org.apache.cassandra.cql3;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;

public interface CQL3Type
{
    public boolean isCollection();
    public boolean isCounter();
    public AbstractType<?> getType();

    public enum Native implements CQL3Type
    {
        ASCII    (AsciiType.instance),
        BIGINT   (LongType.instance),
        BLOB     (BytesType.instance),
        BOOLEAN  (BooleanType.instance),
        COUNTER  (CounterColumnType.instance),
        DECIMAL  (DecimalType.instance),
        DOUBLE   (DoubleType.instance),
        FLOAT    (FloatType.instance),
        INET     (InetAddressType.instance),
        INT      (Int32Type.instance),
        TEXT     (UTF8Type.instance),
        TIMESTAMP(DateType.instance),
        UUID     (UUIDType.instance),
        VARCHAR  (UTF8Type.instance),
        VARINT   (IntegerType.instance),
        TIMEUUID (TimeUUIDType.instance);

        private final AbstractType<?> type;

        private Native(AbstractType<?> type)
        {
            this.type = type;
        }

        public boolean isCollection()
        {
            return false;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        public boolean isCounter()
        {
            return this == COUNTER;
        }

        @Override
        public String toString()
        {
            return super.toString().toLowerCase();
        }
    }

    public static class Custom implements CQL3Type
    {
        private final AbstractType<?> type;

        public Custom(AbstractType<?> type)
        {
            this.type = type;
        }

        public Custom(String className) throws SyntaxException, ConfigurationException
        {
            this(TypeParser.parse(className));
        }

        public boolean isCollection()
        {
            return false;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        public boolean isCounter()
        {
            return false;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Custom))
                return false;

            Custom that = (Custom)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            return "'" + type + "'";
        }
    }

    public static class Collection implements CQL3Type
    {
        CollectionType type;

        public Collection(CollectionType type)
        {
            this.type = type;
        }

        public static Collection map(CQL3Type t1, CQL3Type t2) throws InvalidRequestException
        {
            if (t1.isCollection() || t2.isCollection())
                throw new InvalidRequestException("map type cannot contain another collection");
            if (t1.isCounter() || t2.isCounter())
                throw new InvalidRequestException("counters are not allowed inside a collection");

            return new Collection(MapType.getInstance(t1.getType(), t2.getType()));
        }

        public static Collection list(CQL3Type t) throws InvalidRequestException
        {
            if (t.isCollection())
                throw new InvalidRequestException("list type cannot contain another collection");
            if (t.isCounter())
                throw new InvalidRequestException("counters are not allowed inside a collection");

            return new Collection(ListType.getInstance(t.getType()));
        }

        public static Collection set(CQL3Type t) throws InvalidRequestException
        {
            if (t.isCollection())
                throw new InvalidRequestException("set type cannot contain another collection");
            if (t.isCounter())
                throw new InvalidRequestException("counters are not allowed inside a collection");

            return new Collection(SetType.getInstance(t.getType()));
        }

        public boolean isCollection()
        {
            return true;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        public boolean isCounter()
        {
            return false;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Collection))
                return false;

            Collection that = (Collection)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            switch (type.kind)
            {
                case LIST:
                    return "list<" + ((ListType)type).elements.asCQL3Type() + ">";
                case SET:
                    return "set<" + ((SetType)type).elements.asCQL3Type() + ">";
                case MAP:
                    MapType mt = (MapType)type;
                    return "set<" + mt.keys.asCQL3Type() + ", " + mt.values.asCQL3Type() + ">";
            }
            throw new AssertionError();
        }
    }
}
