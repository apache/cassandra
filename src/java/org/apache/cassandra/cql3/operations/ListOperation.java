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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

public class ListOperation implements Operation
{
    // Our reference time (1 jan 2010, 00:00:00) in milliseconds.
    private static final long REFERENCE_TIME = 1262304000000L;

    /*
     * For prepend, we need to be able to generate unique but decreasing time
     * UUID, which is a bit challenging. To do that, given a time in milliseconds,
     * we adds a number representing the 100-nanoseconds precision and make sure
     * that within the same millisecond, that number is always increasing. We
     * do rely on the fact that the user will only provide decreasing
     * milliseconds timestamp for that purpose.
     */
    private static class PrecisionTime
    {
        public final long millis;
        public final int nanos;

        public PrecisionTime(long millis, int nanos)
        {
            this.millis = millis;
            this.nanos = nanos;
        }
    }

    private static final AtomicReference<PrecisionTime> last = new AtomicReference<PrecisionTime>(new PrecisionTime(Long.MAX_VALUE, 0));

    private static PrecisionTime getNextTime(long millis)
    {
        while (true)
        {
            PrecisionTime current = last.get();

            assert millis <= current.millis;
            PrecisionTime next = millis < current.millis
                    ? new PrecisionTime(millis, 0)
                    : new PrecisionTime(millis, current.nanos + 1);

            if (last.compareAndSet(current, next))
                return next;
        }
    }

    enum Kind { SET, SET_IDX, APPEND, PREPEND, DISCARD, DISCARD_IDX }

    private final List<Term> values;
    private final Kind kind;

    private ListOperation(List<Term> values, Kind kind)
    {
        this.values = Collections.unmodifiableList(values);
        this.kind = kind;
    }

    public void execute(ColumnFamily cf,
                        ColumnNameBuilder builder,
                        CollectionType validator,
                        UpdateParameters params,
                        List<Pair<ByteBuffer, IColumn>> list) throws InvalidRequestException
    {
        if (validator.kind != CollectionType.Kind.LIST)
            throw new InvalidRequestException("List operations are only supported on List typed columns, but " + validator + " given.");

        switch (kind)
        {
            case SET:
                cf.addAtom(params.makeTombstoneForOverwrite(builder.copy().build(), builder.copy().buildAsEndOfRange()));
                doAppend(cf, builder, validator, params);
                break;
            case SET_IDX:
                doSet(cf, builder, params, validator, list);
                break;
            case APPEND:
                doAppend(cf, builder, validator, params);
                break;
            case PREPEND:
                doPrepend(cf, builder, validator, params);
                break;
            case DISCARD:
                doDiscard(cf, validator, params, list);
                break;
            case DISCARD_IDX:
                doDiscardIdx(cf, params, list);
                break;
            default:
                throw new AssertionError("Unsupported List operation: " + kind);
        }
    }

    public void execute(ColumnFamily cf, ColumnNameBuilder builder, AbstractType<?> validator, UpdateParameters params) throws InvalidRequestException
    {
        throw new InvalidRequestException("List operations are only supported on List typed columns, but " + validator + " given.");
    }

    private void doSet(ColumnFamily cf, ColumnNameBuilder builder, UpdateParameters params, CollectionType validator, List<Pair<ByteBuffer, IColumn>> list) throws InvalidRequestException
    {
        int idx = validateListIdx(values.get(0), list);
        Term value = values.get(1);

        ByteBuffer name = list.get(idx).right.name();
        cf.addColumn(params.makeColumn(name, value.getByteBuffer(validator.valueComparator(), params.variables)));
    }

    private void doAppend(ColumnFamily cf, ColumnNameBuilder builder, CollectionType validator, UpdateParameters params) throws InvalidRequestException
    {
        for (int i = 0; i < values.size(); i++)
        {
            ColumnNameBuilder b = i == values.size() - 1 ? builder : builder.copy();
            ByteBuffer uuid = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes());
            ByteBuffer name = b.add(uuid).build();
            cf.addColumn(params.makeColumn(name, values.get(i).getByteBuffer(validator.valueComparator(), params.variables)));
        }
    }

    private void doPrepend(ColumnFamily cf, ColumnNameBuilder builder, CollectionType validator, UpdateParameters params) throws InvalidRequestException
    {
        long time = REFERENCE_TIME - (System.currentTimeMillis() - REFERENCE_TIME);

        // We do the loop in reverse order because getNext() will create increasing time but we want the last
        // value in the prepended list to have the lower time
        for (int i = values.size() - 1; i >= 0; i--)
        {
            ColumnNameBuilder b = i == 0 ? builder : builder.copy();
            PrecisionTime pt = getNextTime(time);
            ByteBuffer uuid = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(pt.millis, pt.nanos));
            ByteBuffer name = b.add(uuid).build();
            cf.addColumn(params.makeColumn(name, values.get(i).getByteBuffer(validator.valueComparator(), params.variables)));
        }
    }

    private void doDiscard(ColumnFamily cf, CollectionType validator, UpdateParameters params, List<Pair<ByteBuffer, IColumn>> list) throws InvalidRequestException
    {
        if (list == null)
            return;

        Set<ByteBuffer> toDiscard = new HashSet<ByteBuffer>();

        for (Term value : values)
            toDiscard.add(value.getByteBuffer(validator.valueComparator(), params.variables));

        for (Pair<ByteBuffer, IColumn> p : list)
        {
            IColumn c = p.right;
            if (toDiscard.contains(c.value()))
                cf.addColumn(params.makeTombstone(c.name()));
        }
    }

    private void doDiscardIdx(ColumnFamily cf, UpdateParameters params, List<Pair<ByteBuffer, IColumn>> list) throws InvalidRequestException
    {
        int idx = validateListIdx(values.get(0), list);
        cf.addColumn(params.makeTombstone(list.get(idx).right.name()));
    }

    public List<Term> getValues()
    {
        return values;
    }

    public boolean requiresRead()
    {
        return kind == Kind.DISCARD || kind == Kind.DISCARD_IDX || kind == Kind.SET || kind == Kind.SET_IDX;
    }

    public Type getType()
    {
        return Type.LIST;
    }

    /* Utility methods */

    public static Operation Set(List<Term> values)
    {
        return new ListOperation(values, Kind.SET);
    }

    public static Operation SetIndex(List<Term> values)
    {
        return new ListOperation(values, Kind.SET_IDX);
    }

    public static Operation Append(List<Term> values)
    {
        return new ListOperation(values, Kind.APPEND);
    }

    public static Operation Prepend(List<Term> values)
    {
        return new ListOperation(values, Kind.PREPEND);
    }

    public static Operation Discard(List<Term> values)
    {
        return new ListOperation(values, Kind.DISCARD);
    }

    public static Operation DiscardKey(List<Term> values)
    {
        return new ListOperation(values, Kind.DISCARD_IDX);
    }

    private int validateListIdx(Term value, List<Pair<ByteBuffer, IColumn>> list) throws InvalidRequestException
    {
        try
        {
            if (value.getType() != Term.Type.INTEGER)
                throw new InvalidRequestException(String.format("Invalid argument %s for %s, must be an integer.", value.getText(), getType()));

            int idx = Integer.parseInt(value.getText());
            if (list == null || list.size() <= idx)
                throw new InvalidRequestException(String.format("Invalid index %d, list has size %d", idx, list == null ? 0 : list.size()));

            return idx;
        }
        catch (NumberFormatException e)
        {
            // This should not happen, unless we screwed up the parser
            throw new RuntimeException();
        }
    }
}
