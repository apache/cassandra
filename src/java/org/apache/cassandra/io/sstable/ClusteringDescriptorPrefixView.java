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

package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * A reusable {@link ClusteringPrefix} view over a {@link ClusteringDescriptor}'s serialized
 * clustering bytes. {@link #reset} wraps the descriptor's array in place and parses the
 * component boundaries; it copies nothing.
 *
 * Only the methods {@link org.apache.cassandra.db.ClusteringComparator#asByteComparable} calls
 * are supported, plus {@link #retainable}: kind, size, get and accessor. Every other method
 * throws {@link UnsupportedOperationException}. get(i) returns one shared window, re-positioned
 * per call, so a caller must consume one component at a time. Use two views to compare two
 * prefixes.
 */
public class ClusteringDescriptorPrefixView implements ClusteringPrefix<ByteBuffer>
{
    private final AbstractType<?>[] types;
    private int size;
    private ClusteringPrefix.Kind kind;
    private int[] offsets = new int[8];
    private int[] lengths = new int[8]; // -1 = null component, 0 = empty
    private byte[] backing;
    private ByteBuffer window;
    private int limit;
    /** True when this view owns its byte copy, so the bytes outlive the descriptor. */
    private boolean owned;

    public ClusteringDescriptorPrefixView(AbstractType<?>[] types)
    {
        this.types = types;
    }

    /**
     * Returns a view that owns a copy of the descriptor's bytes. It stays valid after the
     * descriptor is reused, so a consumer that retains the prefix must use this instead of
     * {@link #reset}.
     */
    public static ClusteringDescriptorPrefixView snapshotOf(ClusteringDescriptor descriptor, AbstractType<?>[] types)
    {
        return snapshot(types,
                        descriptor.clusteringKind(),
                        descriptor.clusteringColumnsBound(),
                        descriptor.clusteringBytes(),
                        descriptor.clusteringLength());
    }

    private static ClusteringDescriptorPrefixView snapshot(AbstractType<?>[] types,
                                                           ClusteringPrefix.Kind kind,
                                                           int size,
                                                           byte[] bytes,
                                                           int length)
    {
        ClusteringDescriptorPrefixView view = new ClusteringDescriptorPrefixView(types);
        byte[] copy = Arrays.copyOf(bytes, length);
        view.kind = kind;
        view.size = size;
        view.owned = true;
        view.backing = copy;
        view.window = ByteBuffer.wrap(copy);
        view.parse(copy.length);
        return view;
    }

    /**
     * Points this view at the descriptor's live bytes and parses them. The view stays correct only
     * until the descriptor is written again, so a consumer that retains it must call
     * {@link #retainable}.
     *
     * @throws IllegalStateException if this view owns a copy, which {@link #snapshotOf} returns
     */
    public ClusteringDescriptorPrefixView reset(ClusteringDescriptor descriptor)
    {
        if (owned)
            throw new IllegalStateException("a snapshot owns its bytes and cannot be reset");

        this.kind = descriptor.clusteringKind();
        this.size = descriptor.clusteringColumnsBound();
        byte[] bytes = descriptor.clusteringBytes();
        int limit = descriptor.clusteringLength();
        if (backing != bytes || window == null)
        {
            backing = bytes;
            window = ByteBuffer.wrap(bytes);
        }
        parse(limit);
        return this;
    }

    // Wire format, as the cursor reader stores it: one vint block header per 32 components
    // (bit 2i = empty, bit 2i+1 = null), then each present component as fixed-width raw bytes,
    // or as a vint length followed by the bytes.
    private void parse(int limit)
    {
        this.limit = limit;
        if (offsets.length < size)
        {
            offsets = new int[size];
            lengths = new int[size];
        }

        int pos = 0;
        long header = 0;
        for (int i = 0; i < size; i++)
        {
            if (i % 32 == 0)
            {
                window.limit(limit).position(pos);
                header = VIntCoding.readUnsignedVInt(window);
                pos = window.position();
            }
            long flags = (header >>> ((i % 32) * 2)) & 0b11;
            if (flags == 0)
            {
                AbstractType<?> type = types[i];
                int len;
                if (type.isValueLengthFixed())
                {
                    len = type.valueLengthIfFixed();
                }
                else
                {
                    window.limit(limit).position(pos);
                    len = (int) VIntCoding.readUnsignedVInt(window);
                    pos = window.position();
                }
                offsets[i] = pos;
                lengths[i] = len;
                pos += len;
            }
            else if ((flags & 0b10) != 0) // null bit (2i+1)
            {
                offsets[i] = pos;
                lengths[i] = -1;
            }
            else // empty bit (2i)
            {
                offsets[i] = pos;
                lengths[i] = 0;
            }
        }
    }

    @Override
    public Kind kind()
    {
        return kind;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public ByteBuffer get(int i)
    {
        if (lengths[i] < 0)
            return null;
        window.limit(offsets[i] + lengths[i]).position(offsets[i]);
        return window;
    }

    @Override
    public ValueAccessor<ByteBuffer> accessor()
    {
        return ByteBufferAccessor.instance;
    }

    @Override
    public String toString(TableMetadata metadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusteringBound<ByteBuffer> asStartBound()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusteringBound<ByteBuffer> asEndBound()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer[] getRawValues()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer[] getBufferArray()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a prefix whose bytes outlive the descriptor. A view that already owns its bytes
     * returns itself.
     */
    @Override
    public ClusteringPrefix<?> retainable()
    {
        return owned ? this : snapshot(types, kind, size, backing, limit);
    }

    @Override
    public long unsharedHeapSize()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusteringPrefix<ByteBuffer> clustering()
    {
        return this;
    }
}
