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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compression.CompressionDictionary.LightweightCompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData.CompressionDictionaryDataObject;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.MBeanWrapper.OnException;

import static java.lang.String.format;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.DEFAULT_TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_VALUE;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.DEFAULT_TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_VALUE;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME;

public class CompressionDictionaryManager implements CompressionDictionaryManagerMBean,
                                                     ICompressionDictionaryCache,
                                                     ICompressionDictionaryEventHandler,
                                                     AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(CompressionDictionaryManager.class);

    private final String keyspaceName;
    private final String tableName;
    private final ColumnFamilyStore columnFamilyStore;
    private volatile boolean mbeanRegistered;
    private volatile boolean isEnabled;

    // Components
    private final ICompressionDictionaryEventHandler eventHandler;
    private final ICompressionDictionaryCache cache;
    private final ICompressionDictionaryScheduler scheduler;
    private ICompressionDictionaryTrainer trainer = null;

    public CompressionDictionaryManager(ColumnFamilyStore columnFamilyStore, boolean registerBookkeeping)
    {
        this.keyspaceName = columnFamilyStore.keyspace.getName();
        this.tableName = columnFamilyStore.getTableName();
        this.columnFamilyStore = columnFamilyStore;

        this.isEnabled = columnFamilyStore.metadata().params.compression.isDictionaryCompressionEnabled();
        this.cache = new CompressionDictionaryCache();
        this.eventHandler = new CompressionDictionaryEventHandler(columnFamilyStore, cache);
        this.scheduler = new CompressionDictionaryScheduler(keyspaceName, tableName, cache, isEnabled);
        if (isEnabled)
        {
            // Initialize components
            this.trainer = ICompressionDictionaryTrainer.create(keyspaceName, tableName,
                                                                columnFamilyStore.metadata().params.compression);
            trainer.setDictionaryTrainedListener(this::handleNewDictionary);

            scheduler.scheduleRefreshTask();

            trainer.start(false, createTrainingConfig());
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
        this.isEnabled = newParams.isDictionaryCompressionEnabled();
        scheduler.setEnabled(isEnabled);
        if (isEnabled)
        {
            registerMbean();
            // Check if we need a new trainer due to compression parameter changes
            boolean needsNewTrainer = shouldCreateNewTrainer(newParams);

            if (needsNewTrainer)
            {
                // Close existing trainer and create a new one
                if (trainer != null)
                {
                    try
                    {
                        trainer.close();
                    }
                    catch (Exception e)
                    {
                        logger.warn("Failed to close existing trainer for {}.{}", keyspaceName, tableName, e);
                    }
                }

                trainer = ICompressionDictionaryTrainer.create(keyspaceName, tableName, newParams);
                trainer.setDictionaryTrainedListener(this::handleNewDictionary);
            }

            scheduler.scheduleRefreshTask();

            // Start trainer if it exists
            if (trainer != null)
            {
                trainer.start(false, createTrainingConfig());
            }
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

    /**
     * Adds a sample to the dictionary trainer for learning compression patterns.
     * Samples are randomly selected to avoid bias and improve dictionary quality.
     *
     * @param sample the sample data to potentially add for training
     */
    public void addSample(ByteBuffer sample)
    {
        ICompressionDictionaryTrainer dictionaryTrainer = trainer;
        if (dictionaryTrainer != null && dictionaryTrainer.shouldSample())
        {
            dictionaryTrainer.addSample(sample);
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
            throw new UnsupportedOperationException("Table " + keyspaceName + '.' + tableName + " does not support dictionary compression");
        }

        if (trainer == null)
        {
            throw new IllegalStateException("Dictionary trainer is not available for table " + keyspaceName + '.' + tableName);
        }

        // resolve training config and fail fast when invalid, so we do not reach logic which would e.g. flush unnecessarily.
        CompressionDictionaryTrainingConfig trainingConfig = createTrainingConfig(parameters);

        // SSTable-based training: sample from existing SSTables
        Set<SSTableReader> sstables = columnFamilyStore.getLiveSSTables();
        if (sstables.isEmpty())
        {
            logger.info("No SSTables available for training in table {}.{}, flushing memtable first",
                        keyspaceName, tableName);
            columnFamilyStore.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
            sstables = columnFamilyStore.getLiveSSTables();

            if (sstables.isEmpty())
            {
                throw new IllegalStateException("No SSTables available for training in table " + keyspaceName + '.' + tableName + " after flush");
            }
        }

        logger.info("Starting SSTable-based training for {}.{} with {} SSTables",
                    keyspaceName, tableName, sstables.size());

        trainer.start(true, trainingConfig);
        scheduler.scheduleSSTableBasedTraining(trainer, sstables, trainingConfig, force);
    }

    @Override
    public CompositeData getTrainingState()
    {
        ICompressionDictionaryTrainer dictionaryTrainer = trainer;
        if (dictionaryTrainer == null)
        {
            return TrainingState.notStarted().toCompositeData();
        }
        return dictionaryTrainer.getTrainingState().toCompositeData();
    }

    @Override
    public TabularData listCompressionDictionaries()
    {
        List<LightweightCompressionDictionary> dictionaries = SystemDistributedKeyspace.retrieveLightweightCompressionDictionaries(keyspaceName, tableName);
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
        CompressionDictionary compressionDictionary = SystemDistributedKeyspace.retrieveLatestCompressionDictionary(keyspaceName, tableName);
        if (compressionDictionary == null)
            return null;

        return CompressionDictionaryDetailsTabularData.fromCompressionDictionary(keyspaceName, tableName, compressionDictionary);
    }

    @Override
    public CompositeData getCompressionDictionary(long dictId)
    {
        CompressionDictionary compressionDictionary = SystemDistributedKeyspace.retrieveCompressionDictionary(keyspaceName, tableName, dictId);
        if (compressionDictionary == null)
            return null;

        return CompressionDictionaryDetailsTabularData.fromCompressionDictionary(keyspaceName, tableName, compressionDictionary);
    }

    @Override
    public synchronized void importCompressionDictionary(CompositeData compositeData)
    {
        if (!isEnabled)
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

        if (trainer.kind() != kind)
        {
            throw new IllegalArgumentException(format("It is not possible to import compression dictionaries of kind " +
                                                      "%s into table %s.%s which supports compression dictionaries of kind %s.",
                                                      kind, keyspaceName, tableName, trainer.kind()));
        }

        CompressionDictionary.DictId dictId = new CompressionDictionary.DictId(kind, dataObject.dictId);

        LightweightCompressionDictionary latestCompressionDictionary = SystemDistributedKeyspace.retrieveLightweightLatestCompressionDictionary(keyspaceName, tableName);
        if (latestCompressionDictionary != null && latestCompressionDictionary.dictId.id > dictId.id)
        {
            throw new IllegalArgumentException(format("Dictionary to import has older dictionary id (%s) than the latest compression dictionary (%s) for table %s.%s",
                                                      dictId.id, latestCompressionDictionary.dictId.id, keyspaceName, tableName));
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
        if (trainer != null)
        {
            closeQuitely(trainer, "CompressionDictionaryTrainer");
            trainer = null;
        }
        closeQuitely(cache, "CompressionDictionaryCache");
        closeQuitely(scheduler, "CompressionDictionaryScheduler");
    }

    private void handleNewDictionary(CompressionDictionary dictionary)
    {
        // sequence meatters; persist the new dictionary before broadcasting to others.
        storeDictionary(dictionary);
        onNewDictionaryTrained(dictionary.dictId());
    }

    /**
     * @return training configuration with max dictionary size and total sample size from CQL table compression params.
     */
    private CompressionDictionaryTrainingConfig createTrainingConfig()
    {
        return createTrainingConfig(Map.of());
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
        CompressionParams compressionParams = columnFamilyStore.metadata().params.compression;
        return CompressionDictionaryTrainingConfig
               .builder()
               .maxDictionarySize(getCompressionDictionaryTrainingMaxDictionarySize(compressionParams, parameters))
               .maxTotalSampleSize(getCompressionDictionaryTrainingMaxTotalSampleSize(compressionParams, parameters))
               .samplingRate(DatabaseDescriptor.getCompressionDictionaryTrainingSamplingRate())
               .chunkSize(compressionParams.chunkLength())
               .build();
    }

    private int getCompressionDictionaryTrainingMaxDictionarySize(CompressionParams compressionParams, Map<String, String> parameters)
    {
        return internalTrainingParameterResolution(compressionParams,
                                                   parameters.get(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME),
                                                   TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME,
                                                   DEFAULT_TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_VALUE);
    }

    private int getCompressionDictionaryTrainingMaxTotalSampleSize(CompressionParams compressionParams, Map<String, String> parameters)
    {
        return internalTrainingParameterResolution(compressionParams,
                                                   parameters.get(TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME),
                                                   TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME,
                                                   DEFAULT_TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_VALUE);
    }

    private int internalTrainingParameterResolution(CompressionParams compressionParams,
                                                    String userSuppliedValue,
                                                    String parameterName,
                                                    String defaultParameterValue)
    {
        String resolvedValue = null;
        try
        {
            if (userSuppliedValue == null)
                resolvedValue = compressionParams.getOtherOptions().getOrDefault(parameterName, defaultParameterValue);
            else
                resolvedValue = userSuppliedValue;

            return new DataStorageSpec.IntKibibytesBound(resolvedValue).toBytes();
        }
        catch (Throwable t)
        {
            throw new IllegalArgumentException(String.format("Invalid value for %s: %s", parameterName, resolvedValue));
        }
    }

    private void storeDictionary(CompressionDictionary dictionary)
    {
        if (!isEnabled)
        {
            return;
        }

        SystemDistributedKeyspace.storeCompressionDictionary(keyspaceName, tableName, dictionary);
        cache.add(dictionary);
    }

    /**
     * Determines if a new trainer should be created based on compression parameter changes.
     * A new trainer is needed when no existing trainer exists or when the existing trainer
     * is not compatible with the new compression parameters.
     * <p>
     * The method is (and should be) only invoked inside {@link #maybeReloadFromSchema(CompressionParams)},
     * which is guarded by synchronized.
     *
     * @param newParams the new compression parameters
     * @return true if a new trainer should be created
     */
    private boolean shouldCreateNewTrainer(CompressionParams newParams)
    {
        if (trainer == null)
        {
            return true;
        }

        return !trainer.isCompatibleWith(newParams);
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

    @VisibleForTesting
    boolean isReady()
    {
        return trainer != null && trainer.isReady();
    }

    @VisibleForTesting
    ICompressionDictionaryTrainer trainer()
    {
        return trainer;
    }
}
