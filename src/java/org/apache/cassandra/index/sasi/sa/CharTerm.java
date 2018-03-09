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
import java.nio.CharBuffer;

import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;
import org.apache.cassandra.db.marshal.AbstractType;

import com.google.common.base.Charsets;

public class CharTerm extends Term<CharBuffer>
{
    public CharTerm(int position, CharBuffer value, TokenTreeBuilder tokens)
    {
        super(position, value, tokens);
    }

    public ByteBuffer getTerm()
    {
        return Charsets.UTF_8.encode(value.duplicate());
    }

    public ByteBuffer getSuffix(int start)
    {
        return Charsets.UTF_8.encode(value.subSequence(value.position() + start, value.remaining()));
    }

    public int compareTo(AbstractType<?> comparator, Term other)
    {
        return value.compareTo((CharBuffer) other.value);
    }

    public int length()
    {
        return value.length();
    }
}
