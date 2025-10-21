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

package org.apache.cassandra.harry.model;

import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ImmutableUniqueList;

public class DetailedTableMetadata
{
    public final TableMetadata metadata;
    public final ImmutableUniqueList<Symbol> partitionColumns;
    public final ImmutableUniqueList<Symbol> clusteringColumns;
    public final ImmutableUniqueList<Symbol> primaryColumns;
    public final ImmutableUniqueList<Symbol> staticColumns;
    public final ImmutableUniqueList<Symbol> regularColumns;
    public final ImmutableUniqueList<Symbol> selectionOrder, partitionAndStaticColumns, clusteringAndRegularColumns, regularAndStaticColumns;

    public DetailedTableMetadata(TableMetadata metadata)
    {
        this.metadata = metadata;
        ImmutableUniqueList.Builder<Symbol> symbolListBuilder = ImmutableUniqueList.builder();
        for (ColumnMetadata pk : metadata.partitionKeyColumns())
            symbolListBuilder.add(Symbol.from(pk));
        partitionColumns = symbolListBuilder.buildAndClear();
        for (ColumnMetadata pk : metadata.clusteringColumns())
            symbolListBuilder.add(Symbol.from(pk));
        clusteringColumns = symbolListBuilder.buildAndClear();
        if (clusteringColumns.isEmpty()) primaryColumns = partitionColumns;
        else
        {
            primaryColumns = symbolListBuilder.addAll(partitionColumns)
                                              .addAll(clusteringColumns)
                                              .buildAndClear();
        }
        metadata.staticColumns().selectOrderIterator().forEachRemaining(cm -> symbolListBuilder.add(Symbol.from(cm)));
        staticColumns = symbolListBuilder.buildAndClear();
        if (staticColumns.isEmpty()) partitionAndStaticColumns = partitionColumns;
        else
        {
            partitionAndStaticColumns = symbolListBuilder.addAll(partitionColumns)
                                                         .addAll(staticColumns)
                                                         .buildAndClear();
        }
        metadata.regularColumns().selectOrderIterator().forEachRemaining(cm -> symbolListBuilder.add(Symbol.from(cm)));
        regularColumns = symbolListBuilder.buildAndClear();
        clusteringAndRegularColumns = symbolListBuilder.addAll(clusteringColumns)
                                                       .addAll(regularColumns)
                                                       .buildAndClear();
        metadata.allColumnsInSelectOrder().forEachRemaining(cm -> symbolListBuilder.add(Symbol.from(cm)));
        selectionOrder = symbolListBuilder.buildAndClear();
        regularAndStaticColumns = symbolListBuilder.addAll(staticColumns).addAll(regularColumns).buildAndClear();
    }

    public Symbol find(String name)
    {
        return selectionOrder.stream().filter(s -> s.symbol.equals(name)).findAny().get();
    }
}
