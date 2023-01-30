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

package org.apache.cassandra.db.virtual;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.IAccordService;

public class AccordVirtualTables
{
    private AccordVirtualTables()
    {

    }

    public static Collection<VirtualTable> getAll(String keyspace)
    {
        if (!DatabaseDescriptor.getAccordTransactionsEnabled())
            return Collections.emptyList();

        return Arrays.asList(
        new Epoch(keyspace)
        );
    }

    @VisibleForTesting
    public static final class Epoch extends AbstractVirtualTable
    {

        protected Epoch(String keyspace)
        {
            super(parse(keyspace, "Accord Epochs",
                        "CREATE TABLE accord_epochs(\n" +
                        "  epoch bigint,\n" +
                        "  PRIMARY KEY ( (epoch) )" +
                        ")"));
        }

        @Override
        public DataSet data()
        {
            IAccordService accord = AccordService.instance();
            accord.createEpochFromConfigUnsafe();

            long epoch = accord.currentEpoch();

            SimpleDataSet result = new SimpleDataSet(metadata());
            result.row(epoch);
            return result;
        }
    }

    private static TableMetadata parse(String keyspace, String comment, String query)
    {
        return CreateTableStatement.parse(query, keyspace)
                                   .comment(comment)
                                   .kind(TableMetadata.Kind.VIRTUAL)
                                   .build();
    }
}
