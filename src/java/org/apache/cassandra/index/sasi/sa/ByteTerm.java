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
package org.apache.cassandra.index.sasi.sa;

import java.nio.ByteBuffer;

import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;
import org.apache.cassandra.db.marshal.AbstractType;

public class ByteTerm extends Term<ByteBuffer>
{
    public ByteTerm(int position, ByteBuffer value, TokenTreeBuilder tokens)
    {
        super(position, value, tokens);
    }

    public ByteBuffer getTerm()
    {
        return value.duplicate();
    }

    public ByteBuffer getSuffix(int start)
    {
        return (ByteBuffer) value.duplicate().position(value.position() + start);
    }

    public int compareTo(AbstractType<?> comparator, Term other)
    {
        return comparator.compare(value, (ByteBuffer) other.value);
    }

    public int length()
    {
        return value.remaining();
    }
}
