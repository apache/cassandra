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

package org.apache.cassandra.exceptions;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Exception thrown when a configured column type is invalid.
 */
public class InvalidColumnTypeException extends ConfigurationException
{
    private final ByteBuffer name;
    private final AbstractType<?> invalidType;
    private final boolean isPrimaryKeyColumn;
    private final boolean isCounterTable;

    public InvalidColumnTypeException(ByteBuffer name,
                                      AbstractType<?> invalidType,
                                      boolean isPrimaryKeyColumn,
                                      boolean isCounterTable,
                                      String reason)
    {
        super(msg(name, invalidType, reason));
        this.name = name;
        this.invalidType = invalidType;
        this.isPrimaryKeyColumn = isPrimaryKeyColumn;
        this.isCounterTable = isCounterTable;
    }

    private static String msg(ByteBuffer name,
                              AbstractType<?> invalidType,
                              String reason)
    {
        return String.format("Invalid type %s for column %s: %s",
                             invalidType.asCQL3Type(),
                             ColumnIdentifier.toCQLString(name),
                             reason);
    }

    /**
     * Attempts to return a "fixed" (and thus valid) version of the type. Doing is so is only possible in restrained
     * case where we know why the type is invalid and are confident we know what it should be.
     *
     * @return if we know how to auto-magically fix the invalid type that triggered this exception, the hopefully
     * fixed version of said type. Otherwise, {@code null}.
     */
    public AbstractType<?> tryFix()
    {
        AbstractType<?> fixed = tryFixInternal();
        if (fixed != null)
        {
            try
            {
                // Make doubly sure the fixed type is valid before returning it.
                fixed.validateForColumn(name, isPrimaryKeyColumn, isCounterTable);
                return fixed;
            }
            catch (InvalidColumnTypeException e2)
            {
                // Continue as if we hadn't been able to fix, since we haven't
            }
        }
        return null;
    }

    private AbstractType<?> tryFixInternal()
    {
        if (isPrimaryKeyColumn)
        {
            // The only issue we have a fix to in that case if the type is not frozen; we can then just freeze it.
            if (invalidType.isMultiCell())
                return invalidType.freeze();
        }
        else
        {
            // Here again, it's mainly issues of frozen-ness that are fixable, namely if a multi-cell type has
            // non-frozen subtypes. In which case, we just freeze all sub-types.
            if (invalidType.isMultiCell())
                return invalidType.with(AbstractType.freeze(invalidType.subTypes()), true);

        }
        // In other case, we don't know how to fix (at least somewhat auto-magically) and will have to fail.
        return null;
    }
}
