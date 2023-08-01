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
package org.apache.cassandra.index.sai.cql.types.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.index.sai.cql.types.DataSet;
import org.apache.cassandra.index.sai.cql.types.QuerySet;

import static org.apache.cassandra.index.sai.cql.types.IndexingTypeSupport.NUMBER_OF_VALUES;

public abstract class CollectionDataSet<T> extends DataSet<T>
{
    public static class SetDataSet<T> extends CollectionDataSet<Set<T>>
    {
        protected final DataSet<T> elementDataSet;

        public SetDataSet(DataSet<T> elementDataSet)
        {
            values = new Set[NUMBER_OF_VALUES];
            this.elementDataSet = elementDataSet;
            for (int index = 0; index < NUMBER_OF_VALUES; index++)
            {
                values[index] = new HashSet<>();
                for (int element = 0; element < getRandom().nextIntBetween(2, 8); element++)
                {
                    values[index].add(elementDataSet.values[getRandom().nextIntBetween(0, elementDataSet.values.length - 1)]);
                }
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.CollectionQuerySet(elementDataSet);
        }

        public String toString()
        {
            return String.format("set<%s>", elementDataSet);
        }
    }

    public static class FrozenSetDataSet<T> extends SetDataSet<T>
    {
        public FrozenSetDataSet(DataSet<T> elementDataSet)
        {
            super(elementDataSet);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.FrozenCollectionQuerySet();
        }

        @Override
        public Collection<String> decorateIndexColumn(String column)
        {
            return Collections.singletonList(String.format("FULL(%s)", column));
        }

        public String toString()
        {
            return String.format("frozen<set<%s>>", elementDataSet);
        }
    }

    public static class ListDataSet<T> extends CollectionDataSet<List<T>>
    {
        protected final DataSet<T> elementDataSet;

        public ListDataSet(DataSet<T> elementDataSet)
        {
            values = new List[NUMBER_OF_VALUES];
            this.elementDataSet = elementDataSet;
            for (int index = 0; index < NUMBER_OF_VALUES; index++)
            {
                values[index] = new ArrayList<>();
                for (int element = 0; element < getRandom().nextIntBetween(2, 8); element++)
                {
                    values[index].add(elementDataSet.values[getRandom().nextIntBetween(0, elementDataSet.values.length - 1)]);
                }
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.CollectionQuerySet(elementDataSet);
        }

        public String toString()
        {
            return String.format("list<%s>", elementDataSet);
        }
    }

    public static class FrozenListDataSet<T> extends ListDataSet<T>
    {
        public FrozenListDataSet(DataSet<T> elementDataSet)
        {
            super(elementDataSet);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.FrozenCollectionQuerySet();
        }

        @Override
        public Collection<String> decorateIndexColumn(String column)
        {
            return Collections.singletonList(String.format("FULL(%s)", column));
        }

        public String toString()
        {
            return String.format("frozen<list<%s>>", elementDataSet);
        }
    }

    public static class MapDataSet<T> extends CollectionDataSet<Map<T, T>>
    {
        protected final DataSet<T> elementDataSet;

        public MapDataSet(DataSet<T> elementDataSet)
        {
            values = new Map[NUMBER_OF_VALUES];
            this.elementDataSet = elementDataSet;
            for (int index = 0; index < NUMBER_OF_VALUES; index++)
            {
                values[index] = new HashMap<>();
                for (int element = 0; element < getRandom().nextIntBetween(2, 8); element++)
                {
                    T key = elementDataSet.values[getRandom().nextIntBetween(0, elementDataSet.values.length - 1)];
                    T value = elementDataSet.values[getRandom().nextIntBetween(0, elementDataSet.values.length - 1)];
                    values[index].put(key, value);
                }
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.MapValuesQuerySet(elementDataSet);
        }

        public String toString()
        {
            return String.format("map<%s,%s>", elementDataSet, elementDataSet);
        }
    }

    public static class FrozenMapValuesDataSet<T> extends MapDataSet<T>
    {
        public FrozenMapValuesDataSet(DataSet<T> elementDataSet)
        {
            super(elementDataSet);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.FrozenCollectionQuerySet();
        }

        @Override
        public Collection<String> decorateIndexColumn(String column)
        {
            return Collections.singletonList(String.format("FULL(%s)", column));
        }

        public String toString()
        {
            return String.format("frozen<map<%s,%s>>", elementDataSet, elementDataSet);
        }
    }

    public static class MapKeysDataSet<T> extends MapDataSet<T>
    {
        public MapKeysDataSet(DataSet<T> elementDataSet)
        {
            super(elementDataSet);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.MapKeysQuerySet(elementDataSet);
        }

        @Override
        public Collection<String> decorateIndexColumn(String column)
        {
            return Collections.singletonList(String.format("KEYS(%s)", column));
        }
    }

    public static class MapValuesDataSet<T> extends MapDataSet<T>
    {
        public MapValuesDataSet(DataSet<T> elementDataSet)
        {
            super(elementDataSet);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.MapValuesQuerySet(elementDataSet);
        }

        @Override
        public Collection<String> decorateIndexColumn(String column)
        {
            return Collections.singletonList(String.format("VALUES(%s)", column));
        }
    }

    public static class MapEntriesDataSet<T> extends MapDataSet<T>
    {
        public MapEntriesDataSet(DataSet<T> elementDataSet)
        {
            super(elementDataSet);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.MapEntriesQuerySet(elementDataSet);
        }

        @Override
        public Collection<String> decorateIndexColumn(String column)
        {
            return Collections.singletonList(String.format("ENTRIES(%s)", column));
        }
    }

    public static class MultiMapDataSet<T> extends MapDataSet<T>
    {
        public MultiMapDataSet(DataSet<T> elementDataSet)
        {
            super(elementDataSet);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.MultiMapQuerySet(elementDataSet);
        }

        @Override
        public Collection<String> decorateIndexColumn(String column)
        {
            return Arrays.asList(String.format("KEYS(%s)", column),
                                 String.format("VALUES(%s)", column),
                                 String.format("ENTRIES(%s)", column));
        }
    }
}
