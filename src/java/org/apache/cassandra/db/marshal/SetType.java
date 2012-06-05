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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class SetType extends CollectionType
{
    private static final Logger logger = LoggerFactory.getLogger(SetType.class);

    // interning instances
    private static final Map<AbstractType<?>, SetType> instances = new HashMap<AbstractType<?>, SetType>();

    public final AbstractType<?> elements;

    public static SetType getInstance(TypeParser parser) throws ConfigurationException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("SetType takes exactly 1 type parameter");

        return getInstance(l.get(0));
    }

    public static synchronized SetType getInstance(AbstractType<?> elements)
    {
        SetType t = instances.get(elements);
        if (t == null)
        {
            t = new SetType(elements);
            instances.put(elements, t);
        }
        return t;
    }

    public SetType(AbstractType<?> elements)
    {
        super(Kind.SET);
        this.elements = elements;
    }

    protected AbstractType<?> nameComparator()
    {
        return elements;
    }

    protected AbstractType<?> valueComparator()
    {
        return EmptyType.instance;
    }

    protected void appendToStringBuilder(StringBuilder sb)
    {
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Collections.<AbstractType<?>>singletonList(elements)));
    }

    public void executeFunction(ColumnFamily cf, ColumnNameBuilder fullPath, Function fct, List<Term> args, UpdateParameters params) throws InvalidRequestException
    {
        switch (fct)
        {
            case ADD:
                doAdd(cf, fullPath, args, params);
                break;
            case DISCARD_SET:
                doDiscard(cf, fullPath, args, params);
                break;
            default:
                throw new AssertionError("Unsupported function " + fct);
        }
    }

    public void doAdd(ColumnFamily cf, ColumnNameBuilder builder, List<Term> values, UpdateParameters params) throws InvalidRequestException
    {
        for (int i = 0; i < values.size(); ++i)
        {
            ColumnNameBuilder b = i == values.size() - 1 ? builder : builder.copy();
            ByteBuffer name = b.add(values.get(i).getByteBuffer(elements, params.variables)).build();
            cf.addColumn(params.makeColumn(name, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        }
    }

    public void doDiscard(ColumnFamily cf, ColumnNameBuilder builder, List<Term> values, UpdateParameters params) throws InvalidRequestException
    {
        for (int i = 0; i < values.size(); ++i)
        {
            ColumnNameBuilder b = i == values.size() - 1 ? builder : builder.copy();
            ByteBuffer name = b.add(values.get(i).getByteBuffer(elements, params.variables)).build();
            cf.addColumn(params.makeTombstone(name));
        }
    }

    public ByteBuffer serializeForThrift(List<Pair<ByteBuffer, IColumn>> columns)
    {
        // We're using a list for now, since json doesn't have maps
        List<Object> l = new ArrayList<Object>(columns.size());
        for (Pair<ByteBuffer, IColumn> p : columns)
            l.add(elements.compose(p.left));
        return ByteBufferUtil.bytes(FBUtilities.json(l));
    }
}
