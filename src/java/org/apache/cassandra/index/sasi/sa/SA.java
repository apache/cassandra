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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder.Mode;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;
import org.apache.cassandra.db.marshal.AbstractType;

public abstract class SA<T extends Buffer>
{
    protected final AbstractType<?> comparator;
    protected final Mode mode;

    protected final List<Term<T>> terms = new ArrayList<>();
    protected int charCount = 0;

    public SA(AbstractType<?> comparator, Mode mode)
    {
        this.comparator = comparator;
        this.mode = mode;
    }

    public Mode getMode()
    {
        return mode;
    }

    public void add(ByteBuffer termValue, TokenTreeBuilder tokens)
    {
        Term<T> term = getTerm(termValue, tokens);
        terms.add(term);
        charCount += term.length();
    }

    public abstract TermIterator finish();

    protected abstract Term<T> getTerm(ByteBuffer termValue, TokenTreeBuilder tokens);
}
