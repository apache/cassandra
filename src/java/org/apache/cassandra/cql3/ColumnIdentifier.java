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
package org.apache.cassandra.cql3;

import java.util.Locale;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.statements.Selectable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Represents an identifer for a CQL column definition.
 */
public class ColumnIdentifier implements Selectable
{
    public final ByteBuffer key;
    private final String text;

    public ColumnIdentifier(String rawText, boolean keepCase)
    {
        this.text = keepCase ? rawText : rawText.toLowerCase(Locale.US);
        this.key = ByteBufferUtil.bytes(this.text);
    }

    public ColumnIdentifier(ByteBuffer key, AbstractType<?> type)
    {
        this.key = key;
        this.text = type.getString(key);
    }

    private ColumnIdentifier(ByteBuffer key, String text)
    {
        this.key = key;
        this.text = text;
    }

    @Override
    public final int hashCode()
    {
        return key.hashCode();
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof ColumnIdentifier))
            return false;
        ColumnIdentifier that = (ColumnIdentifier)o;
        return key.equals(that.key);
    }

    @Override
    public String toString()
    {
        return text;
    }

    /**
     * Because Thrift-created tables may have a non-text comparator, we cannot determine the proper 'key' until
     * we know the comparator. ColumnIdentifier.Raw is a placeholder that can be converted to a real ColumnIdentifier
     * once the comparator is known with prepare(). This should only be used with identifiers that are actual
     * column names. See CASSANDRA-8178 for more background.
     */
    public static class Raw implements Selectable.Raw
    {
        private final String rawText;
        private final String text;

        public Raw(String rawText, boolean keepCase)
        {
            this.rawText = rawText;
            this.text =  keepCase ? rawText : rawText.toLowerCase();
        }

        public ColumnIdentifier prepare(CFMetaData cfm)
        {
            if (cfm.getIsDense() || cfm.comparator instanceof CompositeType || cfm.comparator instanceof UTF8Type)
                return new ColumnIdentifier(text, true);

            // We have a Thrift-created table with a non-text comparator.  We need to parse column names with the comparator
            // to get the correct ByteBuffer representation.  However, this doesn't apply to key aliases, so we need to
            // make a special check for those and treat them normally.  See CASSANDRA-8178.
            ByteBuffer bufferName = ByteBufferUtil.bytes(text);
            for (ColumnDefinition def : cfm.partitionKeyColumns())
            {
                if (def.name.equals(bufferName))
                    return new ColumnIdentifier(text, true);
            }
            return new ColumnIdentifier(cfm.comparator.fromString(rawText), text);
        }

        public boolean processesSelection()
        {
            return false;
        }

        @Override
        public final int hashCode()
        {
            return text.hashCode();
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof ColumnIdentifier.Raw))
                return false;
            ColumnIdentifier.Raw that = (ColumnIdentifier.Raw)o;
            return text.equals(that.text);
        }

        @Override
        public String toString()
        {
            return text;
        }
    }
}
