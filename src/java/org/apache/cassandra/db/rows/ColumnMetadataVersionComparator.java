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
package org.apache.cassandra.db.rows;

import java.util.Comparator;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * A {@code Comparator} use to determine which version of a {@link ColumnMetadata} should be used.
 * <p>
 * We can sometimes get 2 different versions of the definition of a give column due to differing types. This can happen
 * in at least 2 cases:
 * <ul>
 *     <li>for UDT, where new fields can be added (see CASSANDRA-13776).</li>
 *     <li>pre-CASSANDRA-12443, when we allowed type altering. And while we don't allow it anymore, it is possible
 *     to still have sstables with metadata mentioning an old pre-altering type (such old version of pre-altering
 *     types will be eventually eliminated from the system by compaction and thanks to this comparator, but we
 *     cannot guarantee when that's fully done).</li>
 * </ul>
 */
final class ColumnMetadataVersionComparator implements Comparator<ColumnMetadata>
{
    public static final Comparator<ColumnMetadata> INSTANCE = new ColumnMetadataVersionComparator();

    private ColumnMetadataVersionComparator()
    {
    }

    @Override
    public int compare(ColumnMetadata v1, ColumnMetadata v2)
    {
        assert v1.ksName.equals(v2.ksName)
               && v1.cfName.equals(v2.cfName)
               && v1.name.equals(v2.name) : v1.debugString() + " != " + v2.debugString();

        AbstractType<?> v1Type = v1.type;
        AbstractType<?> v2Type = v2.type;

        // In most cases, this is used on equal types, and on most types, equality is cheap (most are singleton classes
        // and just use reference equality), so evacuating that case first.
        if (v1Type.equals(v2Type))
            return 0;

        // If those aren't the same type, one must be "more general" than the other, that is accept strictly more values.
        if (v1Type.isValueCompatibleWith(v2Type))
        {
            // Note: if both accept the same values, there is really no good way to prefer one over the other and so we
            // consider them equal here. In practice, this mean we have 2 types that accepts the same values but are
            // not equal. For internal types, TimestampType/DataType/LongType is, afaik, the only example, but as user
            // can write custom types, who knows when this can happen. But excluding any user custom type weirdness
            // (that would really be a bug of their type), such types should only differ in the way they sort, and as
            // this method is only used for regular/static columns in practice, where sorting has no impact whatsoever,
            // it shouldn't matter too much what we return here.
            return v2Type.isValueCompatibleWith(v1Type) ? 0 : 1;
        }
        else if (v2Type.isValueCompatibleWith(v1Type))
        {
            return -1;
        }
        else
        {
            // Neither is a super type of the other: something is pretty wrong and we probably shouldn't ignore it.
            throw new IllegalArgumentException(String.format("Found 2 incompatible versions of column %s in %s.%s: one " +
                                                             "of type %s and one of type %s (but both types are incompatible)",
                                                             v1.name, v1.ksName, v1.cfName, v1Type, v2Type));
        }
    }
}
