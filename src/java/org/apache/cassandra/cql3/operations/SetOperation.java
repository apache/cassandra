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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class SetOperation implements Operation
{
    enum Kind { SET, ADD, DISCARD }

    private final List<Term> values;
    private final Kind kind;

    private SetOperation(List<Term> values, Kind kind)
    {
        this.values = values;
        this.kind = kind;
    }

    public void execute(ColumnFamily cf,
                        ColumnNameBuilder builder,
                        AbstractType<?> validator,
                        UpdateParameters params,
                        List<Pair<ByteBuffer, Column>> list) throws InvalidRequestException
    {
        if (!(validator instanceof SetType))
            throw new InvalidRequestException("Set operations are only supported on Set typed columns, but " + validator + " given.");

        switch (kind)
        {
            case SET: // fallthrough on purpose; remove previous Set before setting (ADD) the new one
                cf.addAtom(params.makeTombstoneForOverwrite(builder.copy().build(), builder.copy().buildAsEndOfRange()));
            case ADD:
                doAdd(cf, builder, (CollectionType)validator, params);
                break;
            case DISCARD:
                doDiscard(cf, builder, (CollectionType)validator, params);
                break;
            default:
                throw new AssertionError("Unsupported Set operation: " + kind);
        }
    }

    public Operation maybeConvertToEmptyMapOperation()
    {
        // If it's not empty or a DISCARD, it's a proper invalid query, not
        // just the parser that hasn't been able to distinguish empty set from
        // empty map. However, we just this as it will be rejected later and
        // there is no point in duplicating validation
        if (!values.isEmpty())
            return this;

        switch (kind)
        {
            case SET:
                return MapOperation.Set(Collections.<Term, Term>emptyMap());
            case ADD:
                return MapOperation.Put(Collections.<Term, Term>emptyMap());
        }
        return this;
    }

    public static void doSetFromPrepared(ColumnFamily cf, ColumnNameBuilder builder, SetType validator, Term values, UpdateParameters params) throws InvalidRequestException
    {
        if (!values.isBindMarker())
            throw new InvalidRequestException("Can't apply operation on column with " + validator + " type.");

        cf.addAtom(params.makeTombstoneForOverwrite(builder.copy().build(), builder.copy().buildAsEndOfRange()));
        doAddFromPrepared(cf, builder, validator, values, params);
    }

    public static void doAddFromPrepared(ColumnFamily cf, ColumnNameBuilder builder, SetType validator, Term values, UpdateParameters params) throws InvalidRequestException
    {
        if (!values.isBindMarker())
            throw new InvalidRequestException("Can't apply operation on column with " + validator + " type.");

        try
        {
            Set<?> s = validator.compose(params.variables.get(values.bindIndex));
            Iterator<?> iter = s.iterator();
            while (iter.hasNext())
            {
                ColumnNameBuilder b = iter.hasNext() ? builder.copy() : builder;
                ByteBuffer name = b.add(validator.nameComparator().decompose(iter.next())).build();
                cf.addColumn(params.makeColumn(name, ByteBufferUtil.EMPTY_BYTE_BUFFER));
            }
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    public static void doDiscardFromPrepared(ColumnFamily cf, ColumnNameBuilder builder, SetType validator, Term values, UpdateParameters params) throws InvalidRequestException
    {
        if (!values.isBindMarker())
            throw new InvalidRequestException("Can't apply operation on column with " + validator + " type.");

        try
        {
            Set<?> s = validator.compose(params.variables.get(values.bindIndex));
            Iterator<?> iter = s.iterator();
            while (iter.hasNext())
            {
                ColumnNameBuilder b = iter.hasNext() ? builder.copy() : builder;
                ByteBuffer name = b.add(validator.nameComparator().decompose(iter.next())).build();
                cf.addColumn(params.makeTombstone(name));
            }
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    private void doAdd(ColumnFamily cf, ColumnNameBuilder builder, CollectionType validator, UpdateParameters params) throws InvalidRequestException
    {
        for (int i = 0; i < values.size(); ++i)
        {
            ColumnNameBuilder b = i == values.size() - 1 ? builder : builder.copy();
            ByteBuffer name = b.add(values.get(i).getByteBuffer(validator.nameComparator(), params.variables)).build();
            cf.addColumn(params.makeColumn(name, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        }
    }

    private void doDiscard(ColumnFamily cf, ColumnNameBuilder builder, CollectionType validator, UpdateParameters params) throws InvalidRequestException
    {
        for (int i = 0; i < values.size(); ++i)
        {
            ColumnNameBuilder b = i == values.size() - 1 ? builder : builder.copy();
            ByteBuffer name = b.add(values.get(i).getByteBuffer(validator.nameComparator(), params.variables)).build();
            cf.addColumn(params.makeTombstone(name));
        }
    }

    public void addBoundNames(ColumnSpecification column, ColumnSpecification[] boundNames) throws InvalidRequestException
    {
        for (Term t : values)
            if (t.isBindMarker())
                boundNames[t.bindIndex] = column;
    }

    public List<Term> getValues()
    {
        return values;
    }

    public boolean requiresRead(AbstractType<?> validator)
    {
        return false;
    }

    public Type getType()
    {
        return Type.SET;
    }

    /* Utility methods */

    public static Operation Set(List<Term> values)
    {
        return new SetOperation(values, Kind.SET);
    }

    public static Operation Add(List<Term> values)
    {
        return new SetOperation(values, Kind.ADD);
    }

    public static Operation Discard(List<Term> values)
    {
        return new SetOperation(values, Kind.DISCARD);
    }
}
