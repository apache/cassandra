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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.utils.Throwables.merge;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Blocker;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.Transactional;

/**
 * IMPORTANT: When this object is involved in a transactional graph, and is not encapsulated in a LifecycleTransaction,
 * for correct behaviour its commit MUST occur before any others, since it may legitimately fail. This is consistent
 * with the Transactional API, which permits one failing action to occur at the beginning of the commit phase, but also
 * *requires* that the prepareToCommit() phase only take actions that can be rolled back.
 *
 * A class that tracks sstable files involved in a transaction across sstables:
 * if the transaction succeeds the old files should be deleted and the new ones kept; vice-versa if it fails.
 *
 * Two log files, NEW and OLD, contain new and old sstable files respectively. The log files also track each
 * other by referencing each others path in the contents.
 *
 * If the transaction finishes successfully:
 * - the OLD transaction file is deleted along with its contents, this includes the NEW transaction file.
 *   Before deleting we must let the SSTableTidier instances run first for any old readers that are being obsoleted
 *   (mark as compacted) by the transaction, see LifecycleTransaction
 *
 * If the transaction is aborted:
 * - the NEW transaction file and its contents are deleted, this includes the OLD transaction file
 *
 * On start-up:
 * - If we find a NEW transaction file, it means the transaction did not complete and we delete the NEW file and its contents
 * - If we find an OLD transaction file but not a NEW file, it means the transaction must have completed and so we delete
 *   all the contents of the OLD file, if they still exist, and the OLD file itself.
 *
 * See CASSANDRA-7066 for full details.
 */
public class TransactionLogs extends Transactional.AbstractTransactional implements Transactional
{
    private static final Logger logger = LoggerFactory.getLogger(TransactionLogs.class);

    /**
     * A single transaction log file, either NEW or OLD.
     */
    final static class TransactionFile
    {
        static String EXT = ".log";
        static char SEP = '_';
        static String REGEX_STR = String.format("^(.*)_(.*)_(%s|%s)%s$", Type.NEW.txt, Type.OLD.txt, EXT);
        static Pattern REGEX = Pattern.compile(REGEX_STR); //(opname)_(id)_(new|old).data

        public enum Type
        {
            NEW (0, "new"),
            OLD (1, "old");

            public final int idx;
            public final String txt;

            Type(int idx, String txt)
            {
                this.idx = idx;
                this.txt = txt;
            }
        };

        public final Type type;
        public final File file;
        public final TransactionData parent;
        public final Set<String> lines = new HashSet<>();

        public TransactionFile(Type type, TransactionData parent)
        {
            this.type = type;
            this.file = new File(parent.getFileName(type));
            this.parent = parent;

            if (exists())
                lines.addAll(FileUtils.readLines(file));
        }

        public boolean add(SSTable table)
        {
            return add(table.descriptor.baseFilename());
        }

        private boolean add(String path)
        {
            String relativePath = FileUtils.getRelativePath(parent.getParentFolder(), path);
            if (lines.contains(relativePath))
                return false;

            lines.add(relativePath);
            FileUtils.append(file, relativePath);
            return true;
        }

        public void remove(SSTable table)
        {
            String relativePath = FileUtils.getRelativePath(parent.getParentFolder(), table.descriptor.baseFilename());
            assert lines.contains(relativePath) : String.format("%s is not tracked by %s", relativePath, file);

            lines.remove(relativePath);
            delete(relativePath);
        }

        public boolean contains(SSTable table)
        {
            String relativePath = FileUtils.getRelativePath(parent.getParentFolder(), table.descriptor.baseFilename());
            return lines.contains(relativePath);
        }

        private void deleteContents()
        {
            deleteOpposite();

            // we sync the parent file descriptor between opposite log deletion and
            // contents deletion to ensure there is a happens before edge between them
            parent.sync();

            lines.forEach(line -> delete(line));
            lines.clear();
        }

        private void deleteOpposite()
        {
            Type oppositeType = type == Type.NEW ? Type.OLD : Type.NEW;
            String oppositeFile = FileUtils.getRelativePath(parent.getParentFolder(), parent.getFileName(oppositeType));
            assert lines.contains(oppositeFile) : String.format("Could not find %s amongst lines", oppositeFile);

            delete(oppositeFile);
            lines.remove(oppositeFile);
        }

        private void delete(String relativePath)
        {
            getTrackedFiles(relativePath).forEach(file -> TransactionLogs.delete(file));
        }

        public Set<File> getTrackedFiles()
        {
            Set<File> ret = new HashSet<>();
            FileUtils.readLines(file).forEach(line -> ret.addAll(getTrackedFiles(line)));
            ret.add(file);
            return ret;
        }

        private List<File> getTrackedFiles(String relativePath)
        {
            List<File> ret = new ArrayList<>();
            File file = new File(StringUtils.join(parent.getParentFolder(), File.separator, relativePath));
            if (file.exists())
                ret.add(file);
            else
                ret.addAll(Arrays.asList(new File(parent.getParentFolder()).listFiles((dir, name) -> {
                    return name.startsWith(relativePath);
                })));

            return ret;
        }

        public void delete(boolean deleteContents)
        {
            assert file.exists() : String.format("Expected %s to exists", file);

            if (deleteContents)
                deleteContents();

            // we sync the parent file descriptor between contents and log deletion
            // to ensure there is a happens before edge between them
            parent.sync();

            TransactionLogs.delete(file);
        }

        public boolean exists()
        {
            return file.exists();
        }
    }

    /**
     * We split the transaction data from the behavior because we need
     * to reconstruct any left-overs and clean them up, as well as work
     * out which files are temporary. So for these cases we don't want the full
     * transactional behavior, plus it's handy for the TransactionTidier.
     */
    final static class TransactionData implements AutoCloseable
    {
        private final OperationType opType;
        private final UUID id;
        private final File folder;
        private final TransactionFile[] files;
        private int folderDescriptor;
        private boolean succeeded;

        static TransactionData make(File logFile)
        {
            Matcher matcher = TransactionFile.REGEX.matcher(logFile.getName());
            assert matcher.matches();

            OperationType operationType = OperationType.fromFileName(matcher.group(1));
            UUID id = UUID.fromString(matcher.group(2));

            return new TransactionData(operationType, logFile.getParentFile(), id);
        }

        TransactionData(OperationType opType, File folder, UUID id)
        {
            this.opType = opType;
            this.id = id;
            this.folder = folder;
            this.files = new TransactionFile[TransactionFile.Type.values().length];
            for (TransactionFile.Type t : TransactionFile.Type.values())
                this.files[t.idx] = new TransactionFile(t, this);

            this.folderDescriptor = CLibrary.tryOpenDirectory(folder.getPath());
            this.succeeded = !newLog().exists() && oldLog().exists();
        }

        public void succeeded(boolean succeeded)
        {
            this.succeeded = succeeded;
        }

        public void close()
        {
            if (folderDescriptor > 0)
            {
                CLibrary.tryCloseFD(folderDescriptor);
                folderDescriptor = -1;
            }
        }

        void crossReference()
        {
            newLog().add(oldLog().file.getPath());
            oldLog().add(newLog().file.getPath());
        }

        void sync()
        {
            if (folderDescriptor > 0)
                CLibrary.trySync(folderDescriptor);
        }

        TransactionFile newLog()
        {
            return files[TransactionFile.Type.NEW.idx];
        }

        TransactionFile oldLog()
        {
            return files[TransactionFile.Type.OLD.idx];
        }

        OperationType getType()
        {
            return opType;
        }

        UUID getId()
        {
            return id;
        }

        Throwable removeUnfinishedLeftovers(Throwable accumulate)
        {
            try
            {
                if (succeeded)
                    oldLog().delete(true);
                else
                    newLog().delete(true);
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }

            return accumulate;
        }

        Set<File> getTemporaryFiles()
        {
            sync();

            if (newLog().exists())
                return newLog().getTrackedFiles();
            else
                return oldLog().getTrackedFiles();
        }

        String getFileName(TransactionFile.Type type)
        {
            String fileName = StringUtils.join(opType.fileName,
                                               TransactionFile.SEP,
                                               id.toString(),
                                               TransactionFile.SEP,
                                               type.txt,
                                               TransactionFile.EXT);
            return StringUtils.join(folder, File.separator, fileName);
        }

        String getParentFolder()
        {
            return folder.getParent();
        }

        static boolean isLogFile(String name)
        {
            return TransactionFile.REGEX.matcher(name).matches();
        }
    }

    private final Tracker tracker;
    private final TransactionData data;
    private final Ref<TransactionLogs> selfRef;
    // Deleting sstables is tricky because the mmapping might not have been finalized yet,
    // and delete will fail (on Windows) until it is (we only force the unmapping on SUN VMs).
    // Additionally, we need to make sure to delete the data file first, so on restart the others
    // will be recognized as GCable.
    private static final Queue<Runnable> failedDeletions = new ConcurrentLinkedQueue<>();
    private static final Blocker blocker = new Blocker();

    TransactionLogs(OperationType opType, CFMetaData metadata)
    {
        this(opType, metadata, null);
    }

    TransactionLogs(OperationType opType, CFMetaData metadata, Tracker tracker)
    {
        this(opType, new Directories(metadata), tracker);
    }

    TransactionLogs(OperationType opType, Directories directories, Tracker tracker)
    {
        this(opType, directories.getDirectoryForNewSSTables(), tracker);
    }

    TransactionLogs(OperationType opType, File folder, Tracker tracker)
    {
        this.tracker = tracker;
        this.data = new TransactionData(opType,
                                        Directories.getTransactionsDirectory(folder),
                                        UUIDGen.getTimeUUID());
        this.selfRef = new Ref<>(this, new TransactionTidier(data));

        data.crossReference();
        if (logger.isDebugEnabled())
            logger.debug("Created transaction logs with id {}", data.id);
    }

    /**
     * Track a reader as new.
     **/
    void trackNew(SSTable table)
    {
        if (!data.newLog().add(table))
            throw new IllegalStateException(table + " is already tracked as new");

        data.newLog().add(table);
    }

    /**
     * Stop tracking a reader as new.
     */
    void untrackNew(SSTable table)
    {
        data.newLog().remove(table);
    }

    /**
     * Schedule a reader for deletion as soon as it is fully unreferenced and the transaction
     * has been committed.
     */
    SSTableTidier obsoleted(SSTableReader reader)
    {
        if (data.newLog().contains(reader))
        {
            if (data.oldLog().contains(reader))
                throw new IllegalArgumentException();

            return new SSTableTidier(reader, true, this);
        }

        if (!data.oldLog().add(reader))
            throw new IllegalStateException();

        if (tracker != null)
            tracker.notifyDeleting(reader);

        return new SSTableTidier(reader, false, this);
    }

    OperationType getType()
    {
        return data.getType();
    }

    UUID getId()
    {
        return data.getId();
    }

    @VisibleForTesting
    String getDataFolder()
    {
        return data.getParentFolder();
    }

    @VisibleForTesting
    String getLogsFolder()
    {
        return StringUtils.join(getDataFolder(), File.separator, Directories.TRANSACTIONS_SUBDIR);
    }

    @VisibleForTesting
    TransactionData getData()
    {
        return data;
    }

    private static void delete(File file)
    {
        try
        {
            if (logger.isDebugEnabled())
                logger.debug("Deleting {}", file);

            Files.delete(file.toPath());
        }
        catch (NoSuchFileException e)
        {
            logger.warn("Unable to delete {} as it does not exist", file);
        }
        catch (IOException e)
        {
            logger.error("Unable to delete {}", file, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * The transaction tidier.
     *
     * When the transaction reference is fully released we try to delete all the obsolete files
     * depending on the transaction result.
     */
    private static class TransactionTidier implements RefCounted.Tidy, Runnable
    {
        private final TransactionData data;

        public TransactionTidier(TransactionData data)
        {
            this.data = data;
        }

        public void tidy() throws Exception
        {
            run();
        }

        public String name()
        {
            return data.id.toString();
        }

        public void run()
        {
            if (logger.isDebugEnabled())
                logger.debug("Removing files for transaction {}", name());

            Throwable err = data.removeUnfinishedLeftovers(null);

            if (err != null)
            {
                logger.info("Failed deleting files for transaction {}, we'll retry after GC and on on server restart", name(), err);
                failedDeletions.add(this);
            }
            else
            {
                if (logger.isDebugEnabled())
                    logger.debug("Closing file transaction {}", name());
                data.close();
            }
        }
    }

    static class Obsoletion
    {
        final SSTableReader reader;
        final SSTableTidier tidier;

        public Obsoletion(SSTableReader reader, SSTableTidier tidier)
        {
            this.reader = reader;
            this.tidier = tidier;
        }
    }

    /**
     * The SSTableReader tidier. When a reader is fully released and no longer referenced
     * by any one, we run this. It keeps a reference to the parent transaction and releases
     * it when done, so that the final transaction cleanup can run when all obsolete readers
     * are released.
     */
    public static class SSTableTidier implements Runnable
    {
        // must not retain a reference to the SSTableReader, else leak detection cannot kick in
        private final Descriptor desc;
        private final long sizeOnDisk;
        private final Tracker tracker;
        private final boolean wasNew;
        private final Ref<TransactionLogs> parentRef;

        public SSTableTidier(SSTableReader referent, boolean wasNew, TransactionLogs parent)
        {
            this.desc = referent.descriptor;
            this.sizeOnDisk = referent.bytesOnDisk();
            this.tracker = parent.tracker;
            this.wasNew = wasNew;
            this.parentRef = parent.selfRef.tryRef();
        }

        public void run()
        {
            blocker.ask();

            SystemKeyspace.clearSSTableReadMeter(desc.ksname, desc.cfname, desc.generation);

            try
            {
                // If we can't successfully delete the DATA component, set the task to be retried later: see TransactionTidier
                File datafile = new File(desc.filenameFor(Component.DATA));

                delete(datafile);
                // let the remainder be cleaned up by delete
                SSTable.delete(desc, SSTable.discoverComponentsFor(desc));
            }
            catch (Throwable t)
            {
                logger.error("Failed deletion for {}, we'll retry after GC and on server restart", desc);
                failedDeletions.add(this);
                return;
            }

            if (tracker != null && !wasNew)
                tracker.cfstore.metric.totalDiskSpaceUsed.dec(sizeOnDisk);

            // release the referent to the parent so that the all transaction files can be released
            parentRef.release();
        }

        public void abort()
        {
            parentRef.release();
        }
    }

    /**
     * Retry all deletions that failed the first time around (presumably b/c the sstable was still mmap'd.)
     * Useful because there are times when we know GC has been invoked; also exposed as an mbean.
     */
    public static void rescheduleFailedDeletions()
    {
        Runnable task;
        while ( null != (task = failedDeletions.poll()))
            ScheduledExecutors.nonPeriodicTasks.submit(task);
    }

    /**
     * Deletions run on the nonPeriodicTasks executor, (both failedDeletions or global tidiers in SSTableReader)
     * so by scheduling a new empty task and waiting for it we ensure any prior deletion has completed.
     */
    public static void waitForDeletions()
    {
        FBUtilities.waitOnFuture(ScheduledExecutors.nonPeriodicTasks.schedule(() -> {
        }, 0, TimeUnit.MILLISECONDS));
    }

    @VisibleForTesting
    public static void pauseDeletions(boolean stop)
    {
        blocker.block(stop);
    }

    private Throwable complete(Throwable accumulate)
    {
        try
        {
            try
            {
                if (data.succeeded)
                    data.newLog().delete(false);
                else
                    data.oldLog().delete(false);
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }

            accumulate = selfRef.ensureReleased(accumulate);
            return accumulate;
        }
        catch (Throwable t)
        {
            logger.error("Failed to complete file transaction {}", getId(), t);
            return Throwables.merge(accumulate, t);
        }
    }

    protected Throwable doCommit(Throwable accumulate)
    {
        data.succeeded(true);
        return complete(accumulate);
    }

    protected Throwable doAbort(Throwable accumulate)
    {
        data.succeeded(false);
        return complete(accumulate);
    }

    protected void doPrepare() { }

    /**
     * Called on startup to scan existing folders for any unfinished leftovers of
     * operations that were ongoing when the process exited.
     *
     * We check if the new transaction file exists first, and if so we clean it up
     * along with its contents, which includes the old file, else if only the old file exists
     * it means the operation has completed and we only cleanup the old file with its contents.
     */
    static void removeUnfinishedLeftovers(CFMetaData metadata)
    {
        Throwable accumulate = null;
        Set<UUID> ids = new HashSet<>();

        for (File dir : getFolders(metadata, null))
        {
            File[] logs = dir.listFiles((dir1, name) -> {
                return TransactionData.isLogFile(name);
            });

            for (File log : logs)
            {
                try (TransactionData data = TransactionData.make(log))
                {
                    // we need to check this because there are potentially 2 log files per operation
                    if (ids.contains(data.id))
                        continue;

                    ids.add(data.id);
                    accumulate = data.removeUnfinishedLeftovers(accumulate);
                }
            }
        }

        if (accumulate != null)
            logger.error("Failed to remove unfinished transaction leftovers", accumulate);
    }

    /**
     * Return a set of files that are temporary, that is they are involved with
     * a transaction that hasn't completed yet.
     *
     * Only return the files that exist and that are located in the folder
     * specified as a parameter or its sub-folders.
     */
    static Set<File> getTemporaryFiles(CFMetaData metadata, File folder)
    {
        Set<File> ret = new HashSet<>();
        Set<UUID> ids = new HashSet<>();

        for (File dir : getFolders(metadata, folder))
        {
            File[] logs = dir.listFiles((dir1, name) -> {
                return TransactionData.isLogFile(name);
            });

            for (File log : logs)
            {
                try(TransactionData data = TransactionData.make(log))
                {
                    // we need to check this because there are potentially 2 log files per transaction
                    if (ids.contains(data.id))
                        continue;

                    ids.add(data.id);
                    ret.addAll(data.getTemporaryFiles()
                                   .stream()
                                   .filter(file -> FileUtils.isContained(folder, file))
                                   .collect(Collectors.toSet()));
                }
            }
        }

        return ret;
    }

    /**
     * Return the transaction log files that currently exist for this table.
     */
    static Set<File> getLogFiles(CFMetaData metadata)
    {
        Set<File> ret = new HashSet<>();
        for (File dir : getFolders(metadata, null))
            ret.addAll(Arrays.asList(dir.listFiles((dir1, name) -> {
                return TransactionData.isLogFile(name);
            })));

        return ret;
    }

    /**
     * A utility method to work out the existing transaction sub-folders
     * either for a table, or a specific parent folder, or both.
     */
    private static List<File> getFolders(CFMetaData metadata, File folder)
    {
        List<File> ret = new ArrayList<>();
        if (metadata != null)
        {
            Directories directories = new Directories(metadata);
            ret.addAll(directories.getExistingDirectories(Directories.TRANSACTIONS_SUBDIR));
        }

        if (folder != null)
        {
            File opDir = Directories.getExistingDirectory(folder, Directories.TRANSACTIONS_SUBDIR);
            if (opDir != null)
                ret.add(opDir);
        }

        return ret;
    }
}
