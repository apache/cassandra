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

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

final class ScalarFunctionSelector extends AbstractFunctionSelector<ScalarFunction>
{
    static final SelectorDeserializer deserializer = new AbstractFunctionSelectorDeserializer()
    {
        @Override
        protected Selector newFunctionSelector(ProtocolVersion version, Function function, List<Selector> argSelectors)
        {
            return new ScalarFunctionSelector(version, function, argSelectors);
        }
    };

    public void addInput(InputRow input)
    {
        for (int i = 0, m = argSelectors.size(); i < m; i++)
        {
            Selector s = argSelectors.get(i);
            s.addInput(input);
        }
    }

    public void reset()
    {
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion)
    {
        for (int i = 0, m = argSelectors.size(); i < m; i++)
        {
            Selector s = argSelectors.get(i);
            setArg(i, s.getOutput(protocolVersion));
            s.reset();
        }
        return fun.execute(args());
    }

    @Override
    public void validateForGroupBy()
    {
        checkTrue(fun.isMonotonic(), "Only monotonic functions are supported in the GROUP BY clause. Got: %s ", fun);
        for (int i = 0, m = argSelectors.size(); i < m; i++)
            argSelectors.get(i).validateForGroupBy();
    }

    ScalarFunctionSelector(ProtocolVersion version, Function fun, List<Selector> argSelectors)
    {
        super(Kind.SCALAR_FUNCTION_SELECTOR, version, (ScalarFunction) fun, argSelectors);
    }
}
