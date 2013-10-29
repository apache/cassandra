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
package org.apache.cassandra.db.composites;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * A "truly-composite" Composite.
 */
public class CompoundComposite extends AbstractComposite
{
    // We could use a List, but we'll create such object *a lot* and using a array+size is not
    // all that harder, so we save the List object allocation.
    final ByteBuffer[] elements;
    final int size;

    CompoundComposite(ByteBuffer[] elements, int size)
    {
        this.elements = elements;
        this.size = size;
    }

    public int size()
    {
        return size;
    }

    public ByteBuffer get(int i)
    {
        return elements[i];
    }

    protected ByteBuffer[] elementsCopy(Allocator allocator)
    {
        ByteBuffer[] elementsCopy = new ByteBuffer[size];
        for (int i = 0; i < size; i++)
            elementsCopy[i] = allocator.clone(elements[i]);
        return elementsCopy;
    }

    public long memorySize()
    {
        return ObjectSizes.getFieldSize(TypeSizes.NATIVE.sizeof(size))
             + ObjectSizes.getArraySize(elements);
    }

    public Composite copy(Allocator allocator)
    {
        return new CompoundComposite(elementsCopy(allocator), size);
    }
}
