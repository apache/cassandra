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
package org.apache.cassandra.index.sasi.analyzer;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Default noOp tokenizer. The iterator will iterate only once
 * returning the unmodified input
 */
public class NoOpAnalyzer extends AbstractAnalyzer
{
    private ByteBuffer input;
    private boolean hasNext = false;

    public void init(Map<String, String> options, AbstractType validator)
    {}

    public boolean hasNext()
    {
        if (hasNext)
        {
            this.next = input;
            this.hasNext = false;
            return true;
        }
        return false;
    }

    public void reset(ByteBuffer input)
    {
        this.next = null;
        this.input = input;
        this.hasNext = true;
    }
}
