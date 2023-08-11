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

package org.apache.cassandra.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Refs;

public class SSTableImporter
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyStore.class);

    private final ColumnFamilyStore cfs;

    public SSTableImporter(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
    }

    /**
     * Imports sstables from the directories given in options.srcPaths
     *
     * If import fails in any of the directories, that directory is skipped and the failed directories
     * are returned so that the user can re-upload files or remove corrupt files.
     *
     * If one of the directories in srcPaths is not readable/does not exist, we exit immediately to let
     * the user change permissions or similar on the directory.
     *
     * @param options
     * @return list of failed directories
     */
    @VisibleForTesting
    synchronized List<String> importNewSSTables(Options options)
    {
        UUID importID = UUID.randomUUID();
        logger.info("[{}] Loading new SSTables for {}/{}: {}", importID, cfs.getKeyspaceName(), cfs.getTableName(), options);

        List<Pair<Directories.SSTableLister, String>> listers = getSSTableListers(options.srcPaths);

        Set<Descriptor> currentDescriptors = new HashSet<>();
        for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
            currentDescriptors.add(sstable.descriptor);
        List<String> failedDirectories = new ArrayList<>();

        // verify first to avoid starting to copy sstables to the data directories and then have to abort.
        if (options.verifySSTables || options.verifyTokens)
        {
            for (Pair<Directories.SSTableLister, String> listerPair : listers)
            {
                Directories.SSTableLister lister = listerPair.left;
                String dir = listerPair.right;
                for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
                {
                    Descriptor descriptor = entry.getKey();
                    if (!currentDescriptors.contains(entry.getKey()))
                    {
                        try
                        {
                            abortIfDraining();
                            verifySSTableForImport(descriptor, entry.getValue(), options.verifyTokens, options.verifySSTables, options.extendedVerify);
                        }
                        catch (Throwable t)
                        {
                            if (dir != null)
                            {
                                logger.error("[{}] Failed verifying SSTable {} in directory {}", importID, descriptor, dir, t);
                                failedDirectories.add(dir);
                            }
                            else
                            {
                                logger.error("[{}] Failed verifying SSTable {}", importID, descriptor, t);
                                throw new RuntimeException("Failed verifying SSTable " + descriptor, t);
                            }
                            break;
                        }
                    }
                }
            }
        }

        Set<SSTableReader> newSSTables = new HashSet<>();
        for (Pair<Directories.SSTableLister, String> listerPair : listers)
        {
            Directories.SSTableLister lister = listerPair.left;
            String dir = listerPair.right;
            if (failedDirectories.contains(dir))
                continue;

            Set<MovedSSTable> movedSSTables = new HashSet<>();
            Set<SSTableReader> newSSTablesPerDirectory = new HashSet<>();
            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
            {
                try
                {
                    abortIfDraining();
                    Descriptor oldDescriptor = entry.getKey();
                    if (currentDescriptors.contains(oldDescriptor))
                        continue;

                    File targetDir = getTargetDirectory(dir, oldDescriptor, entry.getValue());
                    Descriptor newDescriptor = cfs.getUniqueDescriptorFor(entry.getKey(), targetDir);
                    maybeMutateMetadata(entry.getKey(), options);
                    movedSSTables.add(new MovedSSTable(newDescriptor, entry.getKey(), entry.getValue()));
                    SSTableReader sstable = SSTableReader.moveAndOpenSSTable(cfs, entry.getKey(), newDescriptor, entry.getValue(), options.copyData);
                    newSSTablesPerDirectory.add(sstable);
                }
                catch (Throwable t)
                {
                    newSSTablesPerDirectory.forEach(s -> s.selfRef().release());
                    if (dir != null)
                    {
                        logger.error("[{}] Failed importing sstables in directory {}", importID, dir, t);
                        failedDirectories.add(dir);
                        if (options.copyData)
                        {
                            removeCopiedSSTables(movedSSTables);
                        }
                        else
                        {
                            moveSSTablesBack(movedSSTables);
                        }
                        movedSSTables.clear();
                        newSSTablesPerDirectory.clear();
                        break;
                    }
                    else
                    {
                        logger.error("[{}] Failed importing sstables from data directory - renamed SSTables are: {}", importID, movedSSTables, t);
                        throw new RuntimeException("Failed importing SSTables", t);
                    }
                }
            }
            newSSTables.addAll(newSSTablesPerDirectory);
        }

        if (newSSTables.isEmpty())
        {
            logger.info("[{}] No new SSTables were found for {}/{}", importID, cfs.getKeyspaceName(), cfs.getTableName());
            return failedDirectories;
        }

        logger.info("[{}] Loading new SSTables and building secondary indexes for {}/{}: {}", importID, cfs.getKeyspaceName(), cfs.getTableName(), newSSTables);
        if (logger.isTraceEnabled())
            logLeveling(importID, newSSTables);

        try (Refs<SSTableReader> refs = Refs.ref(newSSTables))
        {
            abortIfDraining();

            // Validate existing SSTable-attached indexes, and then build any that are missing:
            if (!cfs.indexManager.validateSSTableAttachedIndexes(newSSTables, false))
                cfs.indexManager.buildSSTableAttachedIndexesBlocking(newSSTables);

            cfs.getTracker().addSSTables(newSSTables);
            for (SSTableReader reader : newSSTables)
            {
                if (options.invalidateCaches && cfs.isRowCacheEnabled())
                    invalidateCachesForSSTable(reader);
            }
        }
        catch (Throwable t)
        {
            logger.error("[{}] Failed adding SSTables", importID, t);
            throw new RuntimeException("Failed adding SSTables", t);
        }

        logger.info("[{}] Done loading load new SSTables for {}/{}", importID, cfs.getKeyspaceName(), cfs.getTableName());
        return failedDirectories;
    }

    /**
     * Check the state of this node and throws an {@link InterruptedException} if it is currently draining
     *
     * @throws InterruptedException if the node is draining
     */
    private static void abortIfDraining() throws InterruptedException
    {
        if (StorageService.instance.isDraining())
            throw new InterruptedException("SSTables import has been aborted");
    }

    private void logLeveling(UUID importID, Set<SSTableReader> newSSTables)
    {
        StringBuilder sb = new StringBuilder();
        for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
            sb.append(formatMetadata(sstable));
        logger.debug("[{}] Current sstables: {}", importID, sb);
        sb = new StringBuilder();
        for (SSTableReader sstable : newSSTables)
            sb.append(formatMetadata(sstable));
        logger.debug("[{}] New sstables: {}", importID, sb);
    }

    private static String formatMetadata(SSTableReader sstable)
    {
        return String.format("{[%s, %s], %d, %s, %d}",
                             sstable.getFirst().getToken(),
                             sstable.getLast().getToken(),
                             sstable.getSSTableLevel(),
                             sstable.isRepaired(),
                             sstable.onDiskLength());
    }

    /**
     * Opens the sstablereader described by descriptor and figures out the correct directory for it based
     * on the first token
     *
     * srcPath == null means that the sstable is in a data directory and we can use that directly.
     *
     * If we fail figuring out the directory we will pick the one with the most available disk space.
     */
    private File getTargetDirectory(String srcPath, Descriptor descriptor, Set<Component> components)
    {
        if (srcPath == null)
            return descriptor.directory;

        File targetDirectory = null;
        SSTableReader sstable = null;
        try
        {
            sstable = SSTableReader.open(cfs, descriptor, components, cfs.metadata);
            targetDirectory = cfs.getDirectories().getLocationForDisk(cfs.diskBoundaryManager.getDiskBoundaries(cfs).getCorrectDiskForSSTable(sstable));
        }
        finally
        {
            if (sstable != null)
                sstable.selfRef().release();
        }
        return targetDirectory == null ? cfs.getDirectories().getWriteableLocationToLoadFile(descriptor.baseFile()) : targetDirectory;
    }

    /**
     * Create SSTableListers based on srcPaths
     *
     * If srcPaths is empty, we create a lister that lists sstables in the data directories (deprecated use)
     */
    private List<Pair<Directories.SSTableLister, String>> getSSTableListers(Set<String> srcPaths)
    {
        List<Pair<Directories.SSTableLister, String>> listers = new ArrayList<>();

        if (!srcPaths.isEmpty())
        {
            for (String path : srcPaths)
            {
                File dir = new File(path);
                if (!dir.exists())
                {
                    throw new RuntimeException(String.format("Directory %s does not exist", path));
                }
                if (!Directories.verifyFullPermissions(dir, path))
                {
                    throw new RuntimeException("Insufficient permissions on directory " + path);
                }
                listers.add(Pair.create(cfs.getDirectories().sstableLister(dir, Directories.OnTxnErr.IGNORE).skipTemporary(true), path));
            }
        }
        else
        {
            listers.add(Pair.create(cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true), null));
        }

        return listers;
    }

    private static class MovedSSTable
    {
        private final Descriptor newDescriptor;
        private final Descriptor oldDescriptor;
        private final Set<Component> components;

        private MovedSSTable(Descriptor newDescriptor, Descriptor oldDescriptor, Set<Component> components)
        {
            this.newDescriptor = newDescriptor;
            this.oldDescriptor = oldDescriptor;
            this.components = components;
        }

        public String toString()
        {
            return String.format("%s moved to %s with components %s", oldDescriptor, newDescriptor, components);
        }
    }

    /**
     * If we fail when opening the sstable (if for example the user passes in --no-verify and there are corrupt sstables)
     * we might have started copying sstables to the data directory, these need to be moved back to the original name/directory
     */
    private void moveSSTablesBack(Set<MovedSSTable> movedSSTables)
    {
        for (MovedSSTable movedSSTable : movedSSTables)
        {
            if (movedSSTable.newDescriptor.fileFor(Components.DATA).exists())
            {
                logger.debug("Moving sstable {} back to {}", movedSSTable.newDescriptor.fileFor(Components.DATA)
                                                          , movedSSTable.oldDescriptor.fileFor(Components.DATA));
                SSTable.rename(movedSSTable.newDescriptor, movedSSTable.oldDescriptor, movedSSTable.components);
            }
        }
    }

    /**
     * Similarly for moving case, we need to delete all SSTables which were copied already but the
     * copying as a whole has failed so we do not leave any traces behind such failed import.
     *
     * @param movedSSTables tables we have moved already (by copying) which need to be removed
     */
    private void removeCopiedSSTables(Set<MovedSSTable> movedSSTables)
    {
        logger.debug("Removing copied SSTables which were left in data directories after failed SSTable import.");
        for (MovedSSTable movedSSTable : movedSSTables)
        {
            // no logging here as for moveSSTablesBack case above as logging is done in delete method
            movedSSTable.newDescriptor.getFormat().delete(movedSSTable.newDescriptor);
        }
    }

    /**
     * Iterates over all keys in the sstable index and invalidates the row cache
     */
    @VisibleForTesting
    void invalidateCachesForSSTable(SSTableReader reader)
    {
        try (KeyIterator iter = reader.keyIterator())
        {
            while (iter.hasNext())
            {
                DecoratedKey decoratedKey = iter.next();
                cfs.invalidateCachedPartition(decoratedKey);
            }
        }
        catch (IOException ex)
        {
            throw new RuntimeException("Failed to import sstable " + reader.getFilename(), ex);
        }
    }

    /**
     * Verify an sstable for import, throws exception if there is a failure verifying.
     *
     * @param verifyTokens to verify that the tokens are owned by the current node
     * @param verifySSTables to verify the sstables given. If this is false a "quick" verification will be run, just deserializing metadata
     * @param extendedVerify to validate the values in the sstables
     */
    private void verifySSTableForImport(Descriptor descriptor, Set<Component> components, boolean verifyTokens, boolean verifySSTables, boolean extendedVerify)
    {
        SSTableReader reader = null;
        try
        {
            reader = SSTableReader.open(cfs, descriptor, components, cfs.metadata);
            IVerifier.Options verifierOptions = IVerifier.options()
                                                         .extendedVerification(extendedVerify)
                                                         .checkOwnsTokens(verifyTokens)
                                                         .quick(!verifySSTables)
                                                         .invokeDiskFailurePolicy(false)
                                                         .mutateRepairStatus(false).build();

            try (IVerifier verifier = reader.getVerifier(cfs, new OutputHandler.LogOutput(), false, verifierOptions))
            {
                verifier.verify();
            }
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Can't import sstable " + descriptor, t);
        }
        finally
        {
            if (reader != null)
                reader.selfRef().release();
        }
    }

    /**
     * Depending on the options passed in, this might reset level on the sstable to 0 and/or remove the repair information
     * from the sstable
     */
    private void maybeMutateMetadata(Descriptor descriptor, Options options) throws IOException
    {
        if (descriptor.fileFor(Components.STATS).exists())
        {
            if (options.resetLevel)
            {
                descriptor.getMetadataSerializer().mutateLevel(descriptor, 0);
            }
            if (options.clearRepaired)
            {
                descriptor.getMetadataSerializer().mutateRepairMetadata(descriptor, ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                        null,
                                                                        false);
            }
        }
    }

    public static class Options
    {
        private final Set<String> srcPaths;
        private final boolean resetLevel;
        private final boolean clearRepaired;
        private final boolean verifySSTables;
        private final boolean verifyTokens;
        private final boolean invalidateCaches;
        private final boolean extendedVerify;
        private final boolean copyData;

        public Options(Set<String> srcPaths, boolean resetLevel, boolean clearRepaired, boolean verifySSTables, boolean verifyTokens, boolean invalidateCaches, boolean extendedVerify, boolean copyData)
        {
            this.srcPaths = srcPaths;
            this.resetLevel = resetLevel;
            this.clearRepaired = clearRepaired;
            this.verifySSTables = verifySSTables;
            this.verifyTokens = verifyTokens;
            this.invalidateCaches = invalidateCaches;
            this.extendedVerify = extendedVerify;
            this.copyData = copyData;
        }

        public static Builder options(String srcDir)
        {
            return new Builder(Collections.singleton(srcDir));
        }

        public static Builder options(Set<String> srcDirs)
        {
            return new Builder(srcDirs);
        }

        public static Builder options()
        {
            return options(Collections.emptySet());
        }

        @Override
        public String toString()
        {
            return "Options{" +
                   "srcPaths='" + srcPaths + '\'' +
                   ", resetLevel=" + resetLevel +
                   ", clearRepaired=" + clearRepaired +
                   ", verifySSTables=" + verifySSTables +
                   ", verifyTokens=" + verifyTokens +
                   ", invalidateCaches=" + invalidateCaches +
                   ", extendedVerify=" + extendedVerify +
                   ", copyData= " + copyData +
                   '}';
        }

        static class Builder
        {
            private final Set<String> srcPaths;
            private boolean resetLevel = false;
            private boolean clearRepaired = false;
            private boolean verifySSTables = false;
            private boolean verifyTokens = false;
            private boolean invalidateCaches = false;
            private boolean extendedVerify = false;
            private boolean copyData = false;

            private Builder(Set<String> srcPath)
            {
                assert srcPath != null;
                this.srcPaths = srcPath;
            }

            public Builder resetLevel(boolean value)
            {
                resetLevel = value;
                return this;
            }

            public Builder clearRepaired(boolean value)
            {
                clearRepaired = value;
                return this;
            }

            public Builder verifySSTables(boolean value)
            {
                verifySSTables = value;
                return this;
            }

            public Builder verifyTokens(boolean value)
            {
                verifyTokens = value;
                return this;
            }

            public Builder invalidateCaches(boolean value)
            {
                invalidateCaches = value;
                return this;
            }

            public Builder extendedVerify(boolean value)
            {
                extendedVerify = value;
                return this;
            }

            public Builder copyData(boolean value)
            {
                copyData = value;
                return this;
            }

            public Options build()
            {
                return new Options(srcPaths, resetLevel, clearRepaired, verifySSTables, verifyTokens, invalidateCaches, extendedVerify, copyData);
            }
        }
    }

}
