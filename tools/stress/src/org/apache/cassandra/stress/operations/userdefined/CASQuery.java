package org.apache.cassandra.stress.operations.userdefined;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.stress.generate.DistributionFixed;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.Row;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.generate.values.Generator;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CASQuery extends SchemaStatement
{
    private final ImmutableList<Integer> keysIndex;
    private final ImmutableMap<Integer, Integer> casConditionArgFreqMap;
    private final String readQuery;

    private PreparedStatement casReadConditionStatement;

    public CASQuery(Timer timer, StressSettings settings, PartitionGenerator generator, SeedManager seedManager, PreparedStatement statement, ConsistencyLevel cl, ArgSelect argSelect, final String tableName)
    {
        super(timer, settings, new DataSpec(generator, seedManager, new DistributionFixed(1), settings.insert.rowPopulationRatio.get(), argSelect == SchemaStatement.ArgSelect.MULTIROW ? statement.getVariables().size() : 1), statement,
              statement.getVariables().asList().stream().map(ColumnDefinitions.Definition::getName).collect(Collectors.toList()), cl);

        if (argSelect != SchemaStatement.ArgSelect.SAMEROW)
            throw new IllegalArgumentException("CAS is supported only for type 'samerow'");

        ModificationStatement.Parsed modificationStatement;
        try
        {
            modificationStatement = CQLFragmentParser.parseAnyUnhandled(CqlParser::updateStatement,
                    statement.getQueryString());
        }
        catch (RecognitionException e)
        {
            throw new IllegalArgumentException("could not parse update query:" + statement.getQueryString(), e);
        }

        final List<Pair<ColumnIdentifier, ColumnCondition.Raw>> casConditionList = modificationStatement.getConditions();
        List<Integer> casConditionIndex = new ArrayList<>();

        boolean first = true;
        StringBuilder casReadConditionQuery = new StringBuilder();
        casReadConditionQuery.append("SELECT ");
        for (final Pair<ColumnIdentifier, ColumnCondition.Raw> condition : casConditionList)
        {
            if (!condition.right.getValue().getText().equals("?"))
            {
                //condition uses static value, ignore it
                continue;
            }
            if (!first)
            {
                casReadConditionQuery.append(", ");
            }
            casReadConditionQuery.append(condition.left.toString());
            casConditionIndex.add(getDataSpecification().partitionGenerator.indexOf(condition.left.toString()));
            first = false;
        }
        casReadConditionQuery.append(" FROM ").append(tableName).append(" WHERE ");

        first = true;
        ImmutableList.Builder<Integer> keysBuilder = ImmutableList.builder();
        for (final Generator key : getDataSpecification().partitionGenerator.getPartitionKey())
        {
            if (!first)
            {
                casReadConditionQuery.append(" AND ");
            }
            casReadConditionQuery.append(key.name).append(" = ? ");
            keysBuilder.add(getDataSpecification().partitionGenerator.indexOf(key.name));
            first = false;
        }
        for (final Generator clusteringKey : getDataSpecification().partitionGenerator.getClusteringComponents())
        {
            casReadConditionQuery.append(" AND ").append(clusteringKey.name).append(" = ? ");
            keysBuilder.add(getDataSpecification().partitionGenerator.indexOf(clusteringKey.name));
        }
        keysIndex = keysBuilder.build();
        readQuery = casReadConditionQuery.toString();

        ImmutableMap.Builder<Integer, Integer> builder = ImmutableMap.builderWithExpectedSize(casConditionIndex.size());
        for (final Integer oneConditionIndex : casConditionIndex)
        {
            builder.put(oneConditionIndex, Math.toIntExact(Arrays.stream(argumentIndex).filter((x) -> x == oneConditionIndex).count()));
        }
        casConditionArgFreqMap = builder.build();
    }

    private class JavaDriverRun extends Runner
    {
        final JavaDriverClient client;

        private JavaDriverRun(JavaDriverClient client)
        {
            this.client = client;
            casReadConditionStatement = client.prepare(readQuery);
        }

        public boolean run()
        {
            ResultSet rs = client.getSession().execute(bind(client));
            rowCount = rs.all().size();
            partitionCount = Math.min(1, rowCount);
            return true;
        }
    }

    @Override
    public void run(JavaDriverClient client) throws IOException
    {
        timeWithRetry(new JavaDriverRun(client));
    }

    private BoundStatement bind(JavaDriverClient client)
    {
        final Object keys[] = new Object[keysIndex.size()];
        final Row row = getPartitions().get(0).next();

        for (int i = 0; i < keysIndex.size(); i++)
        {
            keys[i] = row.get(keysIndex.get(i));
        }

        //get current db values for all the coluns which are part of dynamic conditions
        ResultSet rs = client.getSession().execute(casReadConditionStatement.bind(keys));
        final Object casDbValues[] = new Object[casConditionArgFreqMap.size()];

        final com.datastax.driver.core.Row casDbValue = rs.one();
        if (casDbValue != null)
        {
            for (int i = 0; i < casConditionArgFreqMap.size(); i++)
            {
                casDbValues[i] = casDbValue.getObject(i);
            }
        }
        //now bind db values for dynamic conditions in actual CAS update operation
        return prepare(row, casDbValues);
    }

    private BoundStatement prepare(final Row row, final Object[] casDbValues)
    {
        final Map<Integer, Integer> localMapping = new HashMap<>(casConditionArgFreqMap);
        int conditionIndexTracker = 0;
        for (int i = 0; i < argumentIndex.length; i++)
        {
            boolean replace = false;
            Integer count = localMapping.get(argumentIndex[i]);
            if (count != null)
            {
                count--;
                localMapping.put(argumentIndex[i], count);
                if (count == 0)
                {
                    replace = true;
                }
            }

            if (replace)
            {
                bindBuffer[i] = casDbValues[conditionIndexTracker++];
            }
            else
            {
                Object value = row.get(argumentIndex[i]);
                if (definitions.getType(i).getName() == DataType.date().getName())
                {
                    // the java driver only accepts com.datastax.driver.core.LocalDate for CQL type "DATE"
                    value = LocalDate.fromDaysSinceEpoch((Integer) value);
                }

                bindBuffer[i] = value;
            }

            if (bindBuffer[i] == null && !getDataSpecification().partitionGenerator.permitNulls(argumentIndex[i]))
            {
                throw new IllegalStateException();
            }
        }
        return statement.bind(bindBuffer);
    }
}
