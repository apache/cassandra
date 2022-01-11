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
package org.apache.cassandra.db.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.LOG_TRANSACTIONS_FACTORY;

/**
 * Factory to create instances used during log transaction processing:
 * - {@link AbstractLogTransaction}: tracks sstable files invovled in a transastion cross sstable.
 * - {@link ILogAwareFileLister}: list files which are not removed by log transactions
 * - {@link ILogFileCleaner}: removes any leftovers from unfinished log transactions
 * - {@link FailedTransactionDeletionHandler}: retries failed log transaction deletions
 */
public interface ILogTransactionsFactory
{
    Logger logger = LoggerFactory.getLogger(ILogTransactionsFactory.class);

    ILogTransactionsFactory instance = !LOG_TRANSACTIONS_FACTORY.isPresent()
                                      ? new LogTransactionsFactory()
                                      : FBUtilities.construct(LOG_TRANSACTIONS_FACTORY.getString(), "log transactions factory");

    /**
     * Create {@link AbstractLogTransaction} that tracks sstable files involved in a transaction across sstables:
     */
    AbstractLogTransaction createLogTransaction(OperationType operationType, TableMetadataRef metadata);

    /**
     * Create {@link ILogAwareFileLister} that lists files which are not removed by log transactions in a folder.
     */
    ILogAwareFileLister createLogAwareFileLister();

    /**
     * Create {@link ILogFileCleaner} that removes any leftovers from unfinished log transactions as indicated by any transaction log files
     */
    ILogFileCleaner createLogFileCleaner();

    /**
     * Create {@link FailedTransactionDeletionHandler} used to retry failed log transaction deletions
     */
    FailedTransactionDeletionHandler createFailedTransactionDeletionHandler();
}
