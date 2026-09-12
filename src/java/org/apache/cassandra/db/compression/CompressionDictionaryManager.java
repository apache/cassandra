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

package org.apache.cassandra.db.compression;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compression.CompressionDictionary.LightweightCompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData.CompressionDictionaryDataObject;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.MBeanWrapper.OnException;

import static java.lang.String.format;
import static org.apache.cassandra.schema.SystemDistributedKeyspace.retrieveLightweightLatestCompressionDictionary;

public class CompressionDictionaryManager implements CompressionDictionaryManagerMBean,
                                                     ICompressionDictionaryCache,
                                                     ICompressionDictionaryEventHandler,
                                                     AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(CompressionDictionaryManager.class);

    private final String keyspaceName;
    private final String tableName;
    private final String tableId;
    private final ColumnFamilyStore columnFamilyStore;
    private volatile boolean mbeanRegistered;
    private volatile boolean isEnabled;
    private volatile CompressionDictionary.Kind kind;
    private volatile CompressionParams compressionParams;

    // Components
    private final ICompressionDictionaryEventHandler eventHandler;
    private final ICompressionDictionaryCache cache;
    private final ICompressionDictionaryScheduler scheduler;

    public CompressionDictionaryManager(ColumnFamilyStore columnFamilyStore, boolean registerBookkeeping)
    {
        this.keyspaceName = columnFamilyStore.keyspace.getName();
        this.tableName = columnFamilyStore.getTableName();
        this.tableId = columnFamilyStore.metadata().id.toLongString();
        this.columnFamilyStore = columnFamilyStore;

        this.compressionParams = columnFamilyStore.metadata().params.compression;
        this.isEnabled = this.compressionParams.isDictionaryCompressionEnabled();
        this.kind = columnFamilyStore.metadata().params.compression.getCompressionDictionaryKind();
        this.cache = new CompressionDictionaryCache();
        this.eventHandler = new CompressionDictionaryEventHandler(columnFamilyStore, cache);
        this.scheduler = new CompressionDictionaryScheduler(keyspaceName, tableName, tableId, cache, isEnabled);
        if (isEnabled)
        {
            scheduler.scheduleRefreshTask();
        }

        if (registerBookkeeping && isEnabled)
        {
            registerMbean();
        }
    }

    static String mbeanName(String keyspaceName, String tableName)
    {
        return MBEAN_NAME + ",keyspace=" + keyspaceName + ",table=" + tableName;
    }

    public boolean isEnabled()
    {
        return isEnabled;
    }

    /**
     * Reloads dictionary management configuration when compression parameters change.
     * This method enables or disables dictionary compression based on the new parameters,
     * and properly manages the lifecycle of training and refresh tasks.
     *
     * @param newParams the new compression parameters to apply
     */
    public synchronized void maybeReloadFromSchema(CompressionParams newParams)
    {
        this.compressionParams = newParams;
        this.isEnabled = compressionParams.isDictionaryCompressionEnabled();
        this.kind = compressionParams.getCompressionDictionaryKind();
        scheduler.setEnabled(isEnabled);
        if (isEnabled)
        {
            registerMbean();
            scheduler.scheduleRefreshTask();
            return;
        }

        // Clean up when dictionary compression is disabled
        try
        {
            close();
        }
        catch (Exception e)
        {
            logger.warn("Failed to close CompressionDictionaryManager on disabling " +
                        "dictionary-based compression for table {}.{}", keyspaceName, tableName);
        }
    }

    @Nullable
    @Override
    public CompressionDictionary getCurrent()
    {
        return cache.getCurrent();
    }

    @Override
    public CompressionDictionary get(CompressionDictionary.DictId dictId)
    {
        return cache.get(dictId);
    }

    @Override
    public void add(@Nullable CompressionDictionary compressionDictionary)
    {
        cache.add(compressionDictionary);
    }

    @Override
    public long cachedDictionariesMemoryUsed()
    {
        return cache.cachedDictionariesMemoryUsed();
    }

    @Override
    public void onNewDictionaryTrained(CompressionDictionary.DictId dictionaryId)
    {
        eventHandler.onNewDictionaryTrained(dictionaryId);
    }

    @Override
    public void onNewDictionaryAvailable(CompressionDictionary.DictId dictionaryId)
    {
        eventHandler.onNewDictionaryAvailable(dictionaryId);
    }

    @Override
    public synchronized void train(boolean force, Map<String, String> parameters)
    {
        // Validate table supports dictionary compression
        if (!isEnabled)
        {
            throw new IllegalStateException(format("The compression on table %s.%s is not enabled or SSTable compressor is not a dictionary compressor.",
                                                   keyspaceName, tableName));
        }

        // resolve training config and fail fast when invalid, so we do not reach logic which would e.g. flush unnecessarily.
        CompressionDictionaryTrainingConfig trainingConfig = createTrainingConfig(parameters);

        LightweightCompressionDictionary dictionary = retrieveLightweightLatestCompressionDictionary(columnFamilyStore.getKeyspaceName(),
                                                                                                     columnFamilyStore.getTableName(),
                                                                                                     columnFamilyStore.metadata.id.toLongString());

        checkTrainingFrequency(dictionary, trainingConfig);

        // SSTable-based training: sample from existing SSTables

        // this is not closed here but in training runnable when finished
        // also, if view is empty, and we throw just below because of it then
        // there is nothing to "release" so close is not necessary
        ColumnFamilyStore.RefViewFragment refViewFragment = columnFamilyStore.selectAndReference(View.selectFunction(SSTableSet.CANONICAL));

        if (refViewFragment.sstables.isEmpty())
        {
            logger.info("No SSTables available for training in table {}.{}, flushing memtable first", keyspaceName, tableName);
            columnFamilyStore.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

            refViewFragment = columnFamilyStore.selectAndReference(View.selectFunction(SSTableSet.CANONICAL));

            if (refViewFragment.sstables.isEmpty())
            {
                throw new IllegalStateException("No SSTables available for training in table " + keyspaceName + '.' + tableName + " after flush");
            }
        }

        scheduler.scheduleSSTableBasedTraining(refViewFragment,
                                               compressionParams,
                                               trainingConfig,
                                               this::handleNewDictionary,
                                               force);
    }

    @Override
    public CompositeData getTrainingState()
    {
        return scheduler.getLastTrainingState().toCompositeData();
    }

    @Override
    public TabularData listCompressionDictionaries()
    {
        List<LightweightCompressionDictionary> dictionaries = SystemDistributedKeyspace.retrieveLightweightCompressionDictionaries(keyspaceName, tableName, tableId);
        TabularDataSupport tableData = new TabularDataSupport(CompressionDictionaryDetailsTabularData.TABULAR_TYPE);

        if (dictionaries == null)
        {
            return tableData;
        }

        for (LightweightCompressionDictionary dictionary : dictionaries)
        {
            tableData.put(CompressionDictionaryDetailsTabularData.fromLightweightCompressionDictionary(dictionary));
        }

        return tableData;
    }

    @Override
    public CompositeData getCompressionDictionary()
    {
        CompressionDictionary compressionDictionary = SystemDistributedKeyspace.retrieveLatestCompressionDictionary(keyspaceName, tableName, tableId);
        if (compressionDictionary == null)
            return null;

        return CompressionDictionaryDetailsTabularData.fromCompressionDictionary(keyspaceName, tableName, tableId, compressionDictionary);
    }

    @Override
    public CompositeData getCompressionDictionary(long dictId)
    {
        CompressionDictionary compressionDictionary = SystemDistributedKeyspace.retrieveCompressionDictionary(keyspaceName, tableName, tableId, dictId);
        if (compressionDictionary == null)
            return null;

        return CompressionDictionaryDetailsTabularData.fromCompressionDictionary(keyspaceName, tableName, tableId, compressionDictionary);
    }

    @Override
    public synchronized void importCompressionDictionary(CompositeData compositeData)
    {
        if (!isEnabled || this.kind == null)
        {
            throw new IllegalStateException(format("The compression on table %s.%s is not enabled or SSTable compressor is not a dictionary compressor.",
                                                   keyspaceName, tableName));
        }

        CompressionDictionaryDataObject dataObject = CompressionDictionaryDetailsTabularData.fromCompositeData(compositeData);

        if (!keyspaceName.equals(dataObject.keyspace) || !tableName.equals(dataObject.table))
            throw new IllegalArgumentException(format("Keyspace and table of a dictionary to import (%s.%s) does not correspond to the keyspace and table this manager is responsible for (%s.%s)",
                                                      dataObject.keyspace, dataObject.table,
                                                      keyspaceName, tableName));

        CompressionDictionary.Kind kind = CompressionDictionary.Kind.valueOf(dataObject.kind);

        if (this.kind != kind)
        {
            throw new IllegalArgumentException(format("It is not possible to import compression dictionaries of kind " +
                                                      "%s into table %s.%s which supports compression dictionaries of kind %s.",
                                                      kind, keyspaceName, tableName, this.kind));
        }

        CompressionDictionary.DictId dictId = new CompressionDictionary.DictId(kind, dataObject.dictId);

        LightweightCompressionDictionary latestCompressionDictionary = retrieveLightweightLatestCompressionDictionary(keyspaceName, tableName, tableId);
        if (latestCompressionDictionary != null)
        {
            if (latestCompressionDictionary.dictId.id > dictId.id)
            {
                throw new IllegalArgumentException(format("Dictionary to import has older dictionary id (%s) than the latest compression dictionary (%s) for table %s.%s",
                                                          dictId.id, latestCompressionDictionary.dictId.id, keyspaceName, tableName));
            }

            checkTrainingFrequency(latestCompressionDictionary, createTrainingConfig(Map.of()));
        }

        handleNewDictionary(kind.createDictionary(dictId, dataObject.dict, dataObject.dictChecksum));
    }

    /**
     * Close all the resources. The method can be called multiple times.
     */
    @Override
    public synchronized void close()
    {
        unregisterMbean();
        closeQuitely(cache, "CompressionDictionaryCache");
        closeQuitely(scheduler, "CompressionDictionaryScheduler");
    }

    void handleNewDictionary(CompressionDictionary dictionary)
    {
        // sequence meatters; persist the new dictionary before broadcasting to others.
        storeDictionary(dictionary);
        onNewDictionaryTrained(dictionary.dictId());
    }

    /**
     * Returns configuration for training where max dictionary size and total sample size can be supplied by a
     * user, e.g. upon the invocation of training method via JMX.
     *
     * @param parameters user-supplied parameters from training, when not specified, CQL compression parameters
     *                   for a given table will be used
     * @return training configuration with max dictionary size and total sample size of supplied arguments.
     */
    private CompressionDictionaryTrainingConfig createTrainingConfig(Map<String, String> parameters)
    {
        return CompressionDictionaryTrainingConfig
               .builder()
               .maxDictionarySize(CompressionDictionaryTrainingConfig.getMaxDictionarySizeWithUserSuppliedParams(compressionParams, parameters))
               .maxTotalSampleSize(CompressionDictionaryTrainingConfig.getMaxTotalSampleSizeWithUserSuppliedParams(compressionParams, parameters))
               .minTrainingFrequency(CompressionDictionaryTrainingConfig.getMinTrainingFrequency(compressionParams.getOtherOptions()))
               .chunkSize(compressionParams.chunkLength())
               .build();
    }


    private void checkTrainingFrequency(LightweightCompressionDictionary lastDictionary, CompressionDictionaryTrainingConfig config)
    {
        Instant lastTraining = lastDictionary == null ? null : lastDictionary.createdAt;

        // if there is no dictionary trained so far or min frequency is 0 - that is we can train as often as we want -
        // then do not check if we can
        if (lastTraining != null && config.minTrainingFrequency != 0)
        {
            Instant now = FBUtilities.now();
            if (lastTraining.isAfter(now.minus(config.minTrainingFrequency, ChronoUnit.MINUTES)))
            {
                Instant nextEarliestTraining = lastTraining.plus(config.minTrainingFrequency, ChronoUnit.MINUTES);
                throw new RuntimeException(format("The next training or importing can occur only at least after %s from the last training which happened at %s. " +
                                                  "You can train again no earlier than at %s.",
                                                  new DurationSpec.IntMinutesBound(config.minTrainingFrequency, TimeUnit.MINUTES),
                                                  lastTraining,
                                                  nextEarliestTraining));
            }
        }
    }

    private void storeDictionary(CompressionDictionary dictionary)
    {
        if (!isEnabled)
        {
            return;
        }

        SystemDistributedKeyspace.storeCompressionDictionary(keyspaceName, tableName, tableId, dictionary);
        cache.add(dictionary);
    }

    private void registerMbean()
    {
        if (!mbeanRegistered)
        {
            MBeanWrapper.instance.registerMBean(this, mbeanName(keyspaceName, tableName));
            mbeanRegistered = true;
        }
    }

    private void unregisterMbean()
    {
        if (mbeanRegistered)
        {
            MBeanWrapper.instance.unregisterMBean(mbeanName(keyspaceName, tableName), OnException.IGNORE);
            mbeanRegistered = false;
        }
    }

    private void closeQuitely(AutoCloseable closeable, String objectName)
    {
        try
        {
            closeable.close();
        }
        catch (Exception exception)
        {
            logger.warn("Failed closing {}", objectName, exception);
        }
    }
}
