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
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Selector representing a simple term (literals or bound variables).
 * <p>
 * Note that we know the term does not include function calls for instance (this is actually enforced by the parser), those
 * being dealt with by their own Selector.
 */
public class TermSelector extends Selector
{
    private final ByteBuffer value;
    private final AbstractType<?> type;

    public static Factory newFactory(final String name, final Term term, final AbstractType<?> type)
    {
        return new Factory()
        {
            protected String getColumnName()
            {
                return name;
            }

            protected AbstractType<?> getReturnType()
            {
                return type;
            }

            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultColumn)
            {
               mapping.addMapping(resultColumn, (ColumnDefinition)null);
            }

            public Selector newInstance(QueryOptions options)
            {
                return new TermSelector(term.bindAndGet(options), type);
            }
        };
    }

    private TermSelector(ByteBuffer value, AbstractType<?> type)
    {
        this.value = value;
        this.type = type;
    }

    public void addInput(ProtocolVersion protocolVersion, Selection.ResultSetBuilder rs) throws InvalidRequestException
    {
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion) throws InvalidRequestException
    {
        return value;
    }

    public AbstractType<?> getType()
    {
        return type;
    }

    public void reset()
    {
    }
}
