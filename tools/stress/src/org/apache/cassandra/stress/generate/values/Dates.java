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
package org.apache.cassandra.stress.generate.values;

import java.util.Date;

import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.settings.OptionDistribution;

public class Dates extends Generator<Date>
{
    public Dates(String name, GeneratorConfig config)
    {
        super(DateType.instance, config, name, Date.class);
    }

    // TODO: let the range of values generated advance as stress test progresses
    @Override
    public Date generate()
    {
        return new Date(identityDistribution.next());
    }

    DistributionFactory defaultIdentityDistribution()
    {
        return OptionDistribution.get("uniform(1.." + Long.toString(50L*365L*24L*60L*60L*1000L) + ")");
    }
}
