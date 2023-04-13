/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.oldlucene;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;

/**
 * A memory-resident {@link Directory} implementation.  Locking
 * implementation is by default the {@link SingleInstanceLockFactory}.
 * 
 * <p><b>Warning:</b> This class is not intended to work with huge
 * indexes. Everything beyond several hundred megabytes will waste
 * resources (GC cycles), because it uses an internal buffer size
 * of 1024 bytes, producing millions of {@code byte[1024]} arrays.
 * This class is optimized for small memory-resident indexes.
 * It also has bad concurrency on multithreaded environments.
 * 
 * <p>It is recommended to materialize large indexes on disk and use
 * {@link MMapDirectory}, which is a high-performance directory
 * implementation working directly on the file system cache of the
 * operating system, so copying data to Java heap space is not useful.
 * 
 * @deprecated This class uses inefficient synchronization and is discouraged
 * in favor of {@link MMapDirectory}. It will be removed in future versions 
 * of Lucene.
 */
@Deprecated
public class RAMDirectory extends BaseDirectory implements Accountable {
  protected final Map<String,RAMFile> fileMap = new ConcurrentHashMap<>();
  protected final AtomicLong sizeInBytes = new AtomicLong();
  
  /** Used to generate temp file names in {@link #createTempOutput}. */
  private final AtomicLong nextTempFileCounter = new AtomicLong();

  /** Constructs an empty {@link Directory}. */
  public RAMDirectory() {
    this(new SingleInstanceLockFactory());
  }

  /** Constructs an empty {@link Directory} with the given {@link LockFactory}. */
  public RAMDirectory(LockFactory lockFactory) {
    super(lockFactory);
  }

  /**
   * Creates a new <code>RAMDirectory</code> instance from a different
   * <code>Directory</code> implementation.  This can be used to load
   * a disk-based index into memory.
   * 
   * <p><b>Warning:</b> This class is not intended to work with huge
   * indexes. Everything beyond several hundred megabytes will waste
   * resources (GC cycles), because it uses an internal buffer size
   * of 1024 bytes, producing millions of {@code byte[1024]} arrays.
   * This class is optimized for small memory-resident indexes.
   * It also has bad concurrency on multithreaded environments.
   * 
   * <p>For disk-based indexes it is recommended to use
   * {@link MMapDirectory}, which is a high-performance directory
   * implementation working directly on the file system cache of the
   * operating system, so copying data to Java heap space is not useful.
   * 
   * <p>Note that the resulting <code>RAMDirectory</code> instance is fully
   * independent from the original <code>Directory</code> (it is a
   * complete copy).  Any subsequent changes to the
   * original <code>Directory</code> will not be visible in the
   * <code>RAMDirectory</code> instance.
   *
   * @param dir a <code>Directory</code> value
   * @exception IOException if an error occurs
   */
  public RAMDirectory(FSDirectory dir, IOContext context) throws IOException {
    this(dir, false, context);
  }
  
  private RAMDirectory(FSDirectory dir, boolean closeDir, IOContext context) throws IOException {
    this();
    for (String file : dir.listAll()) {
      if (!Files.isDirectory(dir.getDirectory().resolve(file))) {
        copyFrom(dir, file, file, context);
      }
    }
    if (closeDir) {
      dir.close();
    }
  }

  @Override
  public final String[] listAll() {
    ensureOpen();
    // NOTE: this returns a "weakly consistent view". Unless we change Dir API, keep this,
    // and do not synchronize or anything stronger. it's great for testing!
    // NOTE: fileMap.keySet().toArray(new String[0]) is broken in non Sun JDKs,
    // and the code below is resilient to map changes during the array population.
    // NOTE: don't replace this with return names.toArray(new String[names.size()]);
    // or some files could be null at the end of the array if files are being deleted
    // concurrently
    Set<String> fileNames = fileMap.keySet();
    List<String> names = new ArrayList<>(fileNames.size());
    for (String name : fileNames) {
      names.add(name);
    }
    String[] namesArray = names.toArray(new String[names.size()]);
    Arrays.sort(namesArray);
    return namesArray;
  }

  public final boolean fileNameExists(String name) {
    ensureOpen();
    return fileMap.containsKey(name);
  }

  /** Returns the length in bytes of a file in the directory.
   * @throws IOException if the file does not exist
   */
  @Override
  public final long fileLength(String name) throws IOException {
    ensureOpen();
    RAMFile file = fileMap.get(name);
    if (file == null) {
      throw new FileNotFoundException(name);
    }
    return file.getLength();
  }
  
  /**
   * Return total size in bytes of all files in this directory. This is
   * currently quantized to RAMOutputStream.BUFFER_SIZE.
   */
  @Override
  public final long ramBytesUsed() {
    ensureOpen();
    return sizeInBytes.get();
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return Accountables.namedAccountables("file", fileMap);
  }
  
  @Override
  public void deleteFile(String name) throws IOException {
    ensureOpen();
    RAMFile file = fileMap.remove(name);
    if (file != null) {
      file.directory = null;
      sizeInBytes.addAndGet(-file.sizeInBytes);
    } else {
      throw new FileNotFoundException(name);
    }
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    RAMFile file = newRAMFile();
    if (fileMap.putIfAbsent(name, file) != null) {
      throw new FileAlreadyExistsException(name);
    }
    return new RAMOutputStream(name, file, true);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    ensureOpen();

    // Make the file first...
    RAMFile file = newRAMFile();

    // ... then try to find a unique name for it:
    while (true) {
      String name = IndexFileNames.segmentFileName(prefix, suffix + "_" + Long.toString(nextTempFileCounter.getAndIncrement(), Character.MAX_RADIX), "tmp");
      if (fileMap.putIfAbsent(name, file) == null) {
        return new RAMOutputStream(name, file, true);
      }
    }
  }

  /**
   * Returns a new {@link RAMFile} for storing data. This method can be
   * overridden to return different {@link RAMFile} impls, that e.g. override
   * {@link RAMFile#newBuffer(int)}.
   */
  protected RAMFile newRAMFile() {
    return new RAMFile(this);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    ensureOpen();
    RAMFile file = fileMap.get(source);
    if (file == null) {
      throw new FileNotFoundException(source);
    }
    if (fileMap.putIfAbsent(dest, file) != null) {
      throw new FileAlreadyExistsException(dest);
    }
    if (!fileMap.remove(source, file)) {
      throw new IllegalStateException("file was unexpectedly replaced: " + source);
    }
    fileMap.remove(source);
  }

  @Override
  public void syncMetaData() throws IOException {
    // we are by definition not durable!
  }
  
  /** Returns a stream reading an existing file. */
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Closes the store to future operations, releasing associated memory. */
  @Override
  public void close() {
    isOpen = false;
    fileMap.clear();
  }

  @Override
  public Set<String> getPendingDeletions() throws IOException
  {
    throw new UnsupportedOperationException();
  }
}
