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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.thrift.InvalidRequestException;

public interface Value
{
    public List<Term> asList();

    public interface CollectionLiteral extends Value
    {
        /**
         * Validate that this literal is compatible with the provided column and
         * throws an InvalidRequestException if not.
         */
        public void validateType(CFDefinition.Name value) throws InvalidRequestException;

        /**
         * Returns whether this litteral is empty or not.
         */
        public boolean isEmpty();

        /**
         * Returns the internal function to use to construct this literal.
         */
        public CollectionType.Function constructionFunction();
    }

    public static class MapLiteral extends HashMap<Term, Term> implements CollectionLiteral
    {
        public List<Term> asList()
        {
            List<Term> l = new ArrayList<Term>(2 * size());
            for (Map.Entry<Term, Term> entry : entrySet())
            {
                l.add(entry.getKey());
                l.add(entry.getValue());
            }
            return l;
        }

        public void validateType(CFDefinition.Name value) throws InvalidRequestException
        {
            if (!(value.type instanceof MapType))
                throw new InvalidRequestException(String.format("Invalid value: %s is not a map", value.name));
        }

        public CollectionType.Function constructionFunction()
        {
            return CollectionType.Function.SET;
        }
    }

    public static class ListLiteral extends ArrayList<Term> implements CollectionLiteral
    {
        public List<Term> asList()
        {
            return this;
        }

        public void validateType(CFDefinition.Name value) throws InvalidRequestException
        {
            if (!(value.type instanceof ListType))
                throw new InvalidRequestException(String.format("Invalid value: %s is not a list", value.name));
        }

        public CollectionType.Function constructionFunction()
        {
            return CollectionType.Function.APPEND;
        }
    }

    public static class SetLiteral extends HashSet<Term> implements CollectionLiteral
    {
        public List<Term> asList()
        {
            return new ArrayList<Term>(this);
        }

        public void validateType(CFDefinition.Name value) throws InvalidRequestException
        {
            // The parser don't distinguish between empty set and empty map and always return an empty set
            if ((value.type instanceof MapType) && !isEmpty())
                throw new InvalidRequestException(String.format("Invalid value: %s is not a map", value.name));
            else if (!(value.type instanceof SetType))
                throw new InvalidRequestException(String.format("Invalid value: %s is not a set", value.name));
        }

        public CollectionType.Function constructionFunction()
        {
            return CollectionType.Function.ADD;
        }
    }
}
