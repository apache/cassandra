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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.Generators;

import static accord.utils.Property.qt;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;
import static org.assertj.core.api.Assertions.assertThat;

public class UntypedResultSetTest
{
    @Test
    public void rowToString()
    {
        qt().forAll(row()).check(row -> {
            String str = row.toString();
            assertThat(str.split(" \\| ")).hasSize(row.getColumns().size());
            assertThat(str).doesNotContain("null");
        });
    }

    @Test
    public void resultSetToString()
    {
        qt().forAll(resultSet().map(UntypedResultSet::create)).check(rs -> {
            String str = rs.toStringUnsafe();
            assertThat(str.split("\n")).describedAs("toStringUnsafe expected to return different size of rows", str).hasSize(rs.size() + 2); // header + footer
        });
    }

    private static Gen<List<ColumnSpecification>> columns()
    {
        Gen<String> identifierGen = fromQT(Generators.IDENTIFIER_GEN);
        // this is testing toString so don't really need a complex type...
        return rs -> {
            int numColumns = rs.nextInt(1, 10);
            String ks = identifierGen.next(rs);
            String tableName = identifierGen.next(rs);
            List<String> names = Gens.lists(identifierGen).unique().ofSize(numColumns).next(rs);
            // rather than generate the type, use a simple type like double as it doesn't matter... the type is not epxected to be parsable, so conflicts in output format doen't matter
            List<AbstractType<?>> types = names.stream().map(ignore -> DoubleType.instance).collect(Collectors.toList());
            List<ColumnSpecification> columns = new ArrayList<>(numColumns);
            for (int i = 0; i < numColumns; i++)
                columns.add(new ColumnSpecification(ks, tableName, new ColumnIdentifier(names.get(i), true), types.get(i)));
            return columns;
        };
    }

    private static Gen<UntypedResultSet.Row> row()
    {
        return columns().flatMap(columns -> rs -> {
            List<ByteBuffer> data = new ArrayList<>(columns.size());
            for (int i = 0; i < columns.size(); i++)
            {
                AbstractTypeGenerators.TypeSupport<?> support = AbstractTypeGenerators.getTypeSupport(columns.get(i).type);
                data.add(fromQT(support.bytesGen()).next(rs));
            }
            return new UntypedResultSet.Row(columns, data);
        });
    }

    private static Gen<ResultSet> resultSet()
    {
        Gen<List<ColumnSpecification>> columnsGen = columns();
        return rs -> {
            ResultSet result = new ResultSet(new ResultSet.ResultMetadata(columnsGen.next(rs)));
            List<Gen<ByteBuffer>> dataGens = result.metadata.names.stream().map(c -> fromQT(AbstractTypeGenerators.getTypeSupport(c.type).bytesGen())).collect(Collectors.toList());
            int numRows = rs.nextInt(0, 10);
            for (int i = 0; i < numRows; i++)
            {
                List<ByteBuffer> row = dataGens.stream().map(g -> g.next(rs)).collect(Collectors.toList());
                result.addRow(row);
            }
            return result;
        };
    }
}