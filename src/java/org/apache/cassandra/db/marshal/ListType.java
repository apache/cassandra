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
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.cassandra.utils.UUIDGen;

public class ListType extends CollectionType
{
    // interning instances
    private static final Map<AbstractType<?>, ListType> instances = new HashMap<AbstractType<?>, ListType>();

    // Our reference time (1 jan 2010, 00:00:00) in milliseconds.
    private static final long REFERENCE_TIME = 1262304000000L;

    /*
     * For prepend, we need to be able to generate unique but decreasing time
     * UUID, which is a bit challenging. To do that, given a time in milliseconds,
     * we adds a number represening the 100-nanoseconds precision and make sure
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

    public final AbstractType<?> elements;

    public static ListType getInstance(TypeParser parser) throws ConfigurationException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("ListType takes exactly 1 type parameter");

        return getInstance(l.get(0));
    }

    public static synchronized ListType getInstance(AbstractType<?> elements)
    {
        ListType t = instances.get(elements);
        if (t == null)
        {
            t = new ListType(elements);
            instances.put(elements, t);
        }
        return t;
    }

    private ListType(AbstractType<?> elements)
    {
        super(Kind.LIST);
        this.elements = elements;
    }

    protected AbstractType<?> nameComparator()
    {
        return TimeUUIDType.instance;
    }

    protected AbstractType<?> valueComparator()
    {
        return elements;
    }

    protected void appendToStringBuilder(StringBuilder sb)
    {
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Collections.<AbstractType<?>>singletonList(elements)));
    }

    public void executeFunction(ColumnFamily cf, ColumnNameBuilder fullPath, Function fct, List<Term> args, UpdateParameters params) throws InvalidRequestException
    {
        switch (fct)
        {
            case APPEND:
                doAppend(cf, fullPath, args, params);
                break;
            case PREPEND:
                doPrepend(cf, fullPath, args, params);
                break;
            default:
                throw new AssertionError("Unsupported function " + fct);
        }
    }

    public void execute(ColumnFamily cf, ColumnNameBuilder fullPath, Function fct, List<Term> args, UpdateParameters params, List<Pair<ByteBuffer, IColumn>> list) throws InvalidRequestException
    {
        switch (fct)
        {
            case SET:
                doSet(cf, fullPath, validateIdx(fct, args.get(0), list), args.get(1), params, list);
                break;
            case DISCARD_LIST:
                // If list is empty, do nothing
                if (list != null)
                    doDiscard(cf, fullPath, args, params, list);
                break;
            case DISCARD_KEY:
                doDiscardIdx(cf, fullPath, validateIdx(fct, args.get(0), list), params, list);
                break;
            default:
                throw new AssertionError();
        }
    }

    private int validateIdx(Function fct, Term value, List<Pair<ByteBuffer, IColumn>> list) throws InvalidRequestException
    {
        try
        {
            if (value.getType() != Term.Type.INTEGER)
                throw new InvalidRequestException(String.format("Invalid argument %s for %s, must be an integer", value.getText(), fct));
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

    private void doPrepend(ColumnFamily cf, ColumnNameBuilder builder, List<Term> values, UpdateParameters params) throws InvalidRequestException
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
            cf.addColumn(params.makeColumn(name, values.get(i).getByteBuffer(elements, params.variables)));
        }
    }

    private void doAppend(ColumnFamily cf, ColumnNameBuilder builder, List<Term> values, UpdateParameters params) throws InvalidRequestException
    {
        for (int i = 0; i < values.size(); i++)
        {
            ColumnNameBuilder b = i == values.size() - 1 ? builder : builder.copy();
            ByteBuffer uuid = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes());
            ByteBuffer name = b.add(uuid).build();
            cf.addColumn(params.makeColumn(name, values.get(i).getByteBuffer(elements, params.variables)));
        }
    }

    public void doSet(ColumnFamily cf, ColumnNameBuilder builder, int idx, Term value, UpdateParameters params, List<Pair<ByteBuffer, IColumn>> list) throws InvalidRequestException
    {
        ByteBuffer name = list.get(idx).right.name();
        cf.addColumn(params.makeColumn(name, value.getByteBuffer(elements, params.variables)));
    }

    public void doDiscard(ColumnFamily cf, ColumnNameBuilder builder, List<Term> values, UpdateParameters params, List<Pair<ByteBuffer, IColumn>> list) throws InvalidRequestException
    {
        Set<ByteBuffer> toDiscard = new HashSet<ByteBuffer>();
        for (Term value : values)
            toDiscard.add(value.getByteBuffer(elements, params.variables));

        for (Pair<ByteBuffer, IColumn> p : list)
        {
            IColumn c = p.right;
            if (toDiscard.contains(c.value()))
                cf.addColumn(params.makeTombstone(c.name()));
        }
    }

    public void doDiscardIdx(ColumnFamily cf, ColumnNameBuilder builder, int idx, UpdateParameters params, List<Pair<ByteBuffer, IColumn>> list) throws InvalidRequestException
    {
        ByteBuffer name = list.get(idx).right.name();
        cf.addColumn(params.makeTombstone(name));
    }

    public ByteBuffer serializeForThrift(List<Pair<ByteBuffer, IColumn>> columns)
    {
        List<Object> l = new ArrayList<Object>(columns.size());
        for (Pair<ByteBuffer, IColumn> p : columns)
            l.add(elements.compose(p.right.value()));
        return ByteBufferUtil.bytes(FBUtilities.json(l));
    }
}
