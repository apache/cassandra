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

package org.apache.cassandra.harry.dsl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.gen.Bijections;
import org.apache.cassandra.harry.gen.DataGenerators;

/**
 * A class that helps to override parts of clustering key. The tricky part about CK overrides is that Harry model makes
 * an assumption about the ordering of clustering keys, which means clusterings have to be sorted in the same way their
 * descriptors are. This, combined with reverse types, makes managing this state somewhat tricky at times.
 *
 * Additionally, Relation in delete/select query receives individual CK descriptors (i.e. after they have been sliced),
 * while most other queries usually operate on inflated clustering key. All of this is required for efficient _stateless_
 * validation, but makes overrides a bit less intuitive.
 *
 * To summarise: overrides for inflating are done for individual clustering key columns. Overrides for deflating a clustering
 * operate on an entire key. Main reason for this is to allow having same string in several rows for the same column.
 */

@SuppressWarnings({"rawtypes", "unchecked"})
public class OverridingCkGenerator extends DataGenerators.KeyGenerator
{
    private final DataGenerators.KeyGenerator delegate;
    private final KeyPartOverride[] columnValueOverrides;
    private final List<ColumnSpec<?>> columnSpecOverrides;
    private final Map<ArrayWrapper, Long> valueToDescriptor;

    // Had to be a static method because you can not call super after you have initialised any fields
    public static OverridingCkGenerator make(DataGenerators.KeyGenerator delegate)
    {
        KeyPartOverride[] columnValueOverrides = new KeyPartOverride[delegate.columns.size()];
        List<ColumnSpec<?>> columnSpecOverrides = new ArrayList<>();
        for (int i = 0; i < delegate.columns.size(); i++)
        {
            columnValueOverrides[i] = new KeyPartOverride<>(delegate.columns.get(i).generator());
            columnSpecOverrides.add(delegate.columns.get(i).override(columnValueOverrides[i]));
        }
        assert columnValueOverrides.length == columnSpecOverrides.size();
        return new OverridingCkGenerator(delegate, columnValueOverrides, columnSpecOverrides);
    }

    private OverridingCkGenerator(DataGenerators.KeyGenerator delegate,
                                  KeyPartOverride[] columnValueOverrides,
                                  List<ColumnSpec<?>> columnSpecOverrides)
    {
        super(columnSpecOverrides);
        this.columnValueOverrides = columnValueOverrides;
        this.columnSpecOverrides = columnSpecOverrides;
        this.delegate = delegate;
        this.valueToDescriptor = new HashMap<>();
    }

    public void override(long descriptor, Object[] value)
    {
        long[] slices = delegate.slice(descriptor);
        for (int i = 0; i < slices.length; i++)
            columnValueOverrides[i].override(slices[i], value[i]);

        // We _always_ deflate clustering key as a package, since we can not validate a clustering key without all components anyways.
        valueToDescriptor.put(new ArrayWrapper(value), descriptor);
    }

    @Override
    public Object[] inflate(long descriptor)
    {
        return DataGenerators.inflateKey(columnSpecOverrides, descriptor, slice(descriptor));
    }

    @Override
    public long deflate(Object[] value)
    {
        Long descriptor = valueToDescriptor.get(new ArrayWrapper(value));
        if (descriptor != null)
            return descriptor;

        return delegate.deflate(value);
    }

    @Override
    public int byteSize()
    {
        return delegate.byteSize();
    }

    @Override
    public int compare(long l, long r)
    {
        return delegate.byteSize();
    }

    @Override
    public long[] slice(long descriptor)
    {
        return delegate.slice(descriptor);
    }

    @Override
    public long stitch(long[] parts)
    {
        return delegate.stitch(parts);
    }

    @Override
    public long minValue(int idx)
    {
        return delegate.minValue(idx);
    }

    @Override
    public long maxValue(int idx)
    {
        return delegate.maxValue(idx);
    }

    public static class KeyPartOverride<T> extends OverridingBijection<T>
    {
        public KeyPartOverride(Bijections.Bijection<T> delegate)
        {
            super(delegate);
        }

        // We do not use deflate for key part overrides
        @Override
        public long deflate(T value)
        {
            throw new IllegalStateException();
        }
    }

}
