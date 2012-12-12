/**
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
package org.apache.cassandra.cql3.operations;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.Pair;

public class MapOperation implements Operation
{
    enum Kind { SET, PUT, DISCARD }

    private final Map<Term, Term> values;
    private final Term discardKey;
    private final Kind kind;

    private MapOperation(Map<Term, Term> values, Kind kind)
    {
        this.values = values;
        this.discardKey = null;
        this.kind = kind;
    }

    private MapOperation(Term discardKey)
    {
        this.values = null;
        this.discardKey = discardKey;
        this.kind = Kind.DISCARD;
    }

    public void execute(ColumnFamily cf,
                        ColumnNameBuilder builder,
                        AbstractType<?> validator,
                        UpdateParameters params,
                        List<Pair<ByteBuffer, Column>> list) throws InvalidRequestException
    {
        if (!(validator instanceof MapType))
            throw new InvalidRequestException("Map operations are only supported on Map typed columns, but " + validator + " given.");

        switch (kind)
        {
            case SET: // fallthrough on purpose; remove previous Map before setting (PUT) the new one
                cf.addAtom(params.makeTombstoneForOverwrite(builder.copy().build(), builder.copy().buildAsEndOfRange()));
            case PUT:
                doPut(cf, builder, (CollectionType)validator, params);
                break;
            case DISCARD:
                doDiscard(cf, builder, (CollectionType)validator, params);
                break;
            default:
                throw new AssertionError("Unsupported Map operation: " + kind);
        }
    }

    public static void doSetFromPrepared(ColumnFamily cf, ColumnNameBuilder builder, MapType validator, Term values, UpdateParameters params) throws InvalidRequestException
    {
        if (!values.isBindMarker())
            throw new InvalidRequestException("Can't apply operation on column with " + validator + " type.");

        cf.addAtom(params.makeTombstoneForOverwrite(builder.copy().build(), builder.copy().buildAsEndOfRange()));
        doPutFromPrepared(cf, builder, validator, values, params);
    }

    public static void doPutFromPrepared(ColumnFamily cf, ColumnNameBuilder builder, MapType validator, Term values, UpdateParameters params) throws InvalidRequestException
    {
        if (!values.isBindMarker())
            throw new InvalidRequestException("Can't apply operation on column with " + validator + " type.");

        try
        {
            Map<?, ?> m = validator.compose(params.variables.get(values.bindIndex));
            for (Map.Entry<?, ?> entry : m.entrySet())
            {
                ByteBuffer name = builder.copy().add(validator.nameComparator().decompose(entry.getKey())).build();
                ByteBuffer value = validator.valueComparator().decompose(entry.getValue());
                cf.addColumn(params.makeColumn(name, value));
            }
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    private void doPut(ColumnFamily cf, ColumnNameBuilder builder, CollectionType validator, UpdateParameters params) throws InvalidRequestException
    {
        for (Map.Entry<Term, Term> entry : values.entrySet())
        {
            ByteBuffer name = builder.copy().add(entry.getKey().getByteBuffer(validator.nameComparator(), params.variables)).build();
            ByteBuffer value = entry.getValue().getByteBuffer(validator.valueComparator(), params.variables);
            cf.addColumn(params.makeColumn(name, value));
        }
    }

    private void doDiscard(ColumnFamily cf, ColumnNameBuilder builder, CollectionType validator, UpdateParameters params) throws InvalidRequestException
    {
        ByteBuffer name = builder.add(discardKey.getByteBuffer(validator.nameComparator(), params.variables)).build();
        cf.addColumn(params.makeTombstone(name));
    }

    public void addBoundNames(ColumnSpecification column, ColumnSpecification[] boundNames) throws InvalidRequestException
    {
        if (!(column.type instanceof MapType))
            throw new InvalidRequestException(String.format("Invalid operation, %s is not of map type", column.name));

        MapType mt = (MapType)column.type;
        for (Map.Entry<Term, Term> entry : values.entrySet())
        {
            Term key = entry.getKey();
            Term value = entry.getValue();
            if (key.isBindMarker())
                boundNames[key.bindIndex] = keySpecOf(column, mt);
            if (value.isBindMarker())
                boundNames[value.bindIndex] = valueSpecOf(column, mt);
        }
    }

    public static ColumnSpecification keySpecOf(ColumnSpecification column, MapType type)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("key(" + column.name + ")", true), type.keys);
    }

    public static ColumnSpecification valueSpecOf(ColumnSpecification column, MapType type)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ")", true), type.values);
    }

    public List<Term> getValues()
    {
        List<Term> l = new ArrayList<Term>(2 * values.size());
        for (Map.Entry<Term, Term> entry : values.entrySet())
        {
            l.add(entry.getKey());
            l.add(entry.getValue());
        }
        return l;
    }

    public boolean requiresRead(AbstractType<?> validator)
    {
        return kind == Kind.SET || kind == Kind.DISCARD;
    }

    public Type getType()
    {
        return Type.MAP;
    }

    /* Utility methods */

    public static Operation Set(Map<Term, Term> values)
    {
        return new MapOperation(values, Kind.SET);
    }

    public static Operation Put(Map<Term, Term> values)
    {
        return new MapOperation(values, Kind.PUT);
    }

    public static Operation Put(final Term key, final Term value)
    {
        return Put(new HashMap<Term, Term>(1) {{ put(key, value); }});
    }

    public static Operation DiscardKey(Term discardKey)
    {
        return new MapOperation(discardKey);
    }
}
