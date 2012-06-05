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

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.thrift.InvalidRequestException;

public interface ParsedType
{
    public boolean isCollection();
    public AbstractType<?> getType();

    public enum Native implements ParsedType
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
    }

    public static class Custom implements ParsedType
    {
        private final AbstractType<?> type;

        public Custom(String className) throws ConfigurationException
        {
            this.type =  TypeParser.parse(className);
        }

        public boolean isCollection()
        {
            return false;
        }

        public AbstractType<?> getType()
        {
            return type;
        }
    }

    public static class Collection implements ParsedType
    {
        CollectionType type;

        private Collection(CollectionType type)
        {
            this.type = type;
        }

        public static Collection map(ParsedType t1, ParsedType t2) throws InvalidRequestException
        {
            if (t1.isCollection() || t2.isCollection())
                throw new InvalidRequestException("map type cannot contain another collection");

            return new Collection(MapType.getInstance(t1.getType(), t2.getType()));
        }

        public static Collection list(ParsedType t) throws InvalidRequestException
        {
            if (t.isCollection())
                throw new InvalidRequestException("list type cannot contain another collection");

            return new Collection(ListType.getInstance(t.getType()));
        }

        public static Collection set(ParsedType t) throws InvalidRequestException
        {
            if (t.isCollection())
                throw new InvalidRequestException("set type cannot contain another collection");

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
    }
}
