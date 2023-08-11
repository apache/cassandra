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
import java.util.List;

import org.apache.cassandra.cql3.functions.AggregateFunction;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

final class AggregateFunctionSelector extends AbstractFunctionSelector<AggregateFunction>
{
    static final SelectorDeserializer deserializer = new AbstractFunctionSelectorDeserializer()
    {
        @Override
        protected Selector newFunctionSelector(ProtocolVersion version, Function function, List<Selector> argSelectors)
        {
            return new AggregateFunctionSelector(version, function, argSelectors);
        }
    };

    private final AggregateFunction.Aggregate aggregate;

    public boolean isAggregate()
    {
        return true;
    }

    public void addInput(InputRow input)
    {
        ProtocolVersion protocolVersion = input.getProtocolVersion();

        // Aggregation of aggregation is not supported
        for (int i = 0, m = argSelectors.size(); i < m; i++)
        {
            Selector s = argSelectors.get(i);
            s.addInput(input);
            setArg(i, s.getOutput(protocolVersion));
            s.reset();
        }
        aggregate.addInput(args());
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion) throws InvalidRequestException
    {
        return aggregate.compute(protocolVersion);
    }

    public void reset()
    {
        aggregate.reset();
    }

    AggregateFunctionSelector(ProtocolVersion version, Function fun, List<Selector> argSelectors) throws InvalidRequestException
    {
        super(Kind.AGGREGATE_FUNCTION_SELECTOR, version, (AggregateFunction) fun, argSelectors);

        this.aggregate = this.fun.newAggregate();
    }
}
