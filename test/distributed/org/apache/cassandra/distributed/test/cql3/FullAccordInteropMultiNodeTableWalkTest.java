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

package org.apache.cassandra.distributed.test.cql3;

import accord.utils.Property;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.consensus.TransactionalMode;

public class FullAccordInteropMultiNodeTableWalkTest extends AccordInteropMultiNodeTableWalkBase
{
    public FullAccordInteropMultiNodeTableWalkTest()
    {
        super(TransactionalMode.full);
    }

    @Override
    protected void preCheck(Cluster cluster, Property.StatefulBuilder builder, Property.CommandsBuilder<State, Void> commandBuilder)
    {
        super.preCheck(cluster, builder, commandBuilder);
        // if a failing seed is detected, populate here
        // Example: builder.withOnlySeed(42L);
        // CQL operations may have operators such as +, -, and / (example 4 + 4), to "apply" them to get a constant value
        // CQL_DEBUG_APPLY_OPERATOR = true;
        // CQL_FORMATTER = CQLFormatter.PrettyPrint::new;

        // Git SHA=7ef111d6e34787c797b7bd219bdec6e9ca080a19
//        builder.withOnlySeed(3448177587462971228L); // list timestamps used wall clock and not execute_at

        // Git SHA=a13c26ac09f344f627fcb9543417a87748d18221
//        builder.withOnlySeed(3448567750659495612L); // wrong error type given

        // Git SHA=14e269b9c5232fdfc5dd7e7d550725ba8dbcef7d
//        builder.withOnlySeed(3653666668640336886L); // still an issue; trying to find more... list is null but expected to be size 2
//        builder.withOnlySeed(3448629695620704295L); // update can do multiple partitions but BEGIN TRANSACTION doesn't support

        // Git SHA=1bcc914d7b4d28ce1cad221cf5c745775cbdd8db
//        builder.withOnlySeed(-4800178716689218479L); // list column should be size 1 but access at 10; but it is null! See org.apache.cassandra.distributed.test.cql3.RepoListReadMissingHistoryTest

//        builder.withOnlySeed(3448619085512145374L); // pds(Select) would fail if the select is a scan rather than pk lookup
    }
}
