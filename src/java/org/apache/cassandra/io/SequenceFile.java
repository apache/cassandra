/**
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

package org.apache.cassandra.io;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.BloomFilter;

import org.apache.log4j.Logger;

/**
 * This class writes key/value pairs sequentially to disk. It is
 * also used to read sequentially from disk. However one could
 * jump to random positions to read data from the file. This class
 * also has many implementations of the IFileWriter and IFileReader
 * interfaces which are exposed through factory methods.
 * <p/>
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com ) & Karthik Ranganathan ( kranganathan@facebook.com )
 */

public class SequenceFile
{
    public static abstract class AbstractWriter implements IFileWriter
    {
        protected String filename_;

        AbstractWriter(String filename)
        {
            filename_ = filename;
        }

        public String getFileName()
        {
            return filename_;
        }

        public long lastModified()
        {
            File file = new File(filename_);
            return file.lastModified();
        }
    }

    public static class Writer extends AbstractWriter
    {
        protected RandomAccessFile file_;

        Writer(String filename) throws IOException
        {
            super(filename);
            init(filename);
        }

        Writer(String filename, int size) throws IOException
        {
            super(filename);
            init(filename, size);
        }

        protected void init(String filename) throws IOException
        {
            File file = new File(filename);
            if (!file.exists())
            {
                file.createNewFile();
            }
            file_ = new RandomAccessFile(file, "rw");
        }

        protected void init(String filename, int size) throws IOException
        {
            init(filename);
        }

        public long getCurrentPosition() throws IOException
        {
            return file_.getFilePointer();
        }

        public void seek(long position) throws IOException
        {
            file_.seek(position);
        }

        public void append(DataOutputBuffer buffer) throws IOException
        {
            file_.write(buffer.getData(), 0, buffer.getLength());
        }

        public void append(DataOutputBuffer keyBuffer, DataOutputBuffer buffer) throws IOException
        {
            int keyBufLength = keyBuffer.getLength();
            if (keyBuffer == null || keyBufLength == 0)
                throw new IllegalArgumentException("Key cannot be NULL or of zero length.");

            file_.writeInt(keyBufLength);
            file_.write(keyBuffer.getData(), 0, keyBufLength);

            int length = buffer.getLength();
            file_.writeInt(length);
            file_.write(buffer.getData(), 0, length);
        }

        public void append(String key, DataOutputBuffer buffer) throws IOException
        {
            if (key == null)
                throw new IllegalArgumentException("Key cannot be NULL.");

            file_.writeUTF(key);
            int length = buffer.getLength();
            file_.writeInt(length);
            file_.write(buffer.getData(), 0, length);
        }

        public void append(String key, byte[] value) throws IOException
        {
            if (key == null)
                throw new IllegalArgumentException("Key cannot be NULL.");

            file_.writeUTF(key);
            file_.writeInt(value.length);
            file_.write(value);
        }

        public void append(String key, long value) throws IOException
        {
            if (key == null)
                throw new IllegalArgumentException("Key cannot be NULL.");

            file_.writeUTF(key);
            file_.writeLong(value);
        }

        /**
         * Be extremely careful while using this API. This currently
         * used to write the commit log header in the commit logs.
         * If not used carefully it could completely screw up reads
         * of other key/value pairs that are written.
         *
         * @param bytes the bytes to write
         */
        public long writeDirect(byte[] bytes) throws IOException
        {
            file_.write(bytes);
            return file_.getFilePointer();
        }

        public void writeLong(long value) throws IOException
        {
            file_.writeLong(value);
        }

        public void close() throws IOException
        {
            file_.getChannel().force(true);
            file_.close();
        }

        public void close(byte[] footer, int size) throws IOException
        {
            file_.writeUTF(SequenceFile.marker_);
            file_.writeInt(size);
            file_.write(footer, 0, size);
        }

        public String getFileName()
        {
            return filename_;
        }

        public long getFileSize() throws IOException
        {
            return file_.length();
        }
    }

    public static class BufferWriter extends Writer
    {

        BufferWriter(String filename, int size) throws IOException
        {
            super(filename, size);
        }

        @Override
        protected void init(String filename) throws IOException
        {
            init(filename, 0);
        }

        @Override
        protected void init(String filename, int size) throws IOException
        {
            File file = new File(filename);
            file_ = new BufferedRandomAccessFile(file, "rw", size);
            if (!file.exists())
            {
                file.createNewFile();
            }
        }
    }


    /**
     *  This is a reader that finds the block for a starting column and returns
     *  blocks before/after it for each next call. This function assumes that
     *  the CF is sorted by name and exploits the name index.
     */
    public static class ColumnGroupReader extends BufferReader
    {
        private String key_;
        private String cfName_;
        private String cfType_;
        private int indexType_;
        private boolean isAscending_;

        private List<IndexHelper.ColumnIndexInfo> columnIndexList_;
        private long columnStartPosition_;
        private int curRangeIndex_;
        private int allColumnsSize_;
        private int localDeletionTime_;
        private long markedForDeleteAt_;

        ColumnGroupReader(String filename, String key, String cfName, String startColumn, boolean isAscending, long position) throws IOException
        {
            super(filename, 128 * 1024);
            this.cfName_ = cfName;
            this.key_ = key;
            this.isAscending_ = isAscending;
            init(startColumn, position);
        }

        /**
         *   Build a list of index entries ready for search.
         */
        private List<IndexHelper.ColumnIndexInfo> getFullColumnIndexList(List<IndexHelper.ColumnIndexInfo> columnIndexList, int totalNumCols)
        {
            if (columnIndexList.size() == 0)
            {
                /* if there is no column index, add an index entry that covers the full space. */
                return Arrays.asList(new IndexHelper.ColumnIndexInfo[]{new IndexHelper.ColumnNameIndexInfo("", 0, totalNumCols)});
            }

            List<IndexHelper.ColumnIndexInfo> fullColIndexList = new ArrayList<IndexHelper.ColumnIndexInfo>();
            int accumulatededCols = 0;
            for (IndexHelper.ColumnIndexInfo colPosInfo : columnIndexList)
                accumulatededCols += colPosInfo.count();
            int remainingCols = totalNumCols - accumulatededCols;

            fullColIndexList.add(new IndexHelper.ColumnNameIndexInfo("", 0, columnIndexList.get(0).count()));
            for (int i = 0; i < columnIndexList.size() - 1; i++)
            {
                IndexHelper.ColumnNameIndexInfo colPosInfo = (IndexHelper.ColumnNameIndexInfo)columnIndexList.get(i);
                fullColIndexList.add(new IndexHelper.ColumnNameIndexInfo(colPosInfo.name(),
                                                                         colPosInfo.position(),
                                                                         columnIndexList.get(i + 1).count()));
            }
            String columnName = ((IndexHelper.ColumnNameIndexInfo)columnIndexList.get(columnIndexList.size() - 1)).name();
            fullColIndexList.add(new IndexHelper.ColumnNameIndexInfo(columnName,
                                                                     columnIndexList.get(columnIndexList.size() - 1).position(),
                                                                     remainingCols));
            return fullColIndexList;
        }

        private void init(String startColumn, long position) throws IOException
        {
            String keyInDisk = null;
            if (seekTo(position) >= 0)
                keyInDisk = file_.readUTF();

            if ( keyInDisk != null && keyInDisk.equals(key_))
            {
                /* read off the size of this row */
                int dataSize = file_.readInt();
                /* skip the bloomfilter */
                int totalBytesRead = IndexHelper.skipBloomFilter(file_);
                /* read off the index flag, it has to be true */
                boolean hasColumnIndexes = file_.readBoolean();
                totalBytesRead += 1;

                /* read the index */
                List<IndexHelper.ColumnIndexInfo> colIndexList = new ArrayList<IndexHelper.ColumnIndexInfo>();
                if (hasColumnIndexes)
                    totalBytesRead += IndexHelper.deserializeIndex(getTableName(), cfName_, file_, colIndexList);

                /* need to do two things here.
                 * 1. move the file pointer to the beginning of the list of stored columns
                 * 2. calculate the size of all columns */
                String cfName = file_.readUTF();
                cfType_ = file_.readUTF();
                indexType_ = file_.readInt();
                localDeletionTime_ = file_.readInt();
                markedForDeleteAt_ = file_.readLong();
                int totalNumCols = file_.readInt();
                allColumnsSize_ = dataSize - (totalBytesRead + 2 * utfPrefix_ + cfName.length() + cfType_.length() + 4 + 4 + 8 + 4);

                columnStartPosition_ = file_.getFilePointer();
                columnIndexList_ = getFullColumnIndexList(colIndexList, totalNumCols);

                int index = Collections.binarySearch(columnIndexList_, new IndexHelper.ColumnNameIndexInfo(startColumn));
                curRangeIndex_ = index < 0 ? (++index) * (-1) - 1 : index;
            }
            else
            {
                /* no keys found in this file because of a false positive in BF */
                curRangeIndex_ = -1;
                columnIndexList_ = new ArrayList<IndexHelper.ColumnIndexInfo>();
            }
        }

        private boolean getBlockFromCurIndex(DataOutputBuffer bufOut) throws IOException
        {
            if (curRangeIndex_ < 0 || curRangeIndex_ >= columnIndexList_.size())
                return false;
            IndexHelper.ColumnIndexInfo curColPostion = columnIndexList_.get(curRangeIndex_);
            long start = curColPostion.position();
            long end = curRangeIndex_ < columnIndexList_.size() - 1
                       ? columnIndexList_.get(curRangeIndex_+1).position()
                       : allColumnsSize_;

            /* seek to the correct offset to the data, and calculate the data size */
            file_.seek(columnStartPosition_ + start);
            long dataSize = end - start;

            bufOut.reset();
            // write CF info
            bufOut.writeUTF(cfName_);
            bufOut.writeUTF(cfType_);
            bufOut.writeInt(indexType_);
            bufOut.writeInt(localDeletionTime_);
            bufOut.writeLong(markedForDeleteAt_);
            // now write the columns
            bufOut.writeInt(curColPostion.count());
            bufOut.write(file_, (int)dataSize);
            return true;
        }

        public boolean getNextBlock(DataOutputBuffer outBuf) throws IOException
        {
            boolean result = getBlockFromCurIndex(outBuf);
            if (isAscending_)
                curRangeIndex_++;
            else
                curRangeIndex_--;
            return result;
        }
    }

    public static abstract class AbstractReader implements IFileReader
    {
        private static final short utfPrefix_ = 2;
        protected RandomAccessFile file_;
        protected String filename_;

        AbstractReader(String filename)
        {
            filename_ = filename;
        }

        String getTableName()
        {
            return SSTable.parseTableName(filename_);
        }

        public String getFileName()
        {
            return filename_;
        }

        long seekTo(long position) throws IOException
        {
            if (position >= 0)
                seek(position);
            return position;
        }

        /**
         * Defreeze the bloom filter.
         *
         * @return bloom filter summarizing the column information
         * @throws IOException
         */
        private BloomFilter defreezeBloomFilter() throws IOException
        {
            int size = file_.readInt();
            byte[] bytes = new byte[size];
            file_.readFully(bytes);
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(bytes, bytes.length);
            BloomFilter bf = BloomFilter.serializer().deserialize(bufIn);
            return bf;
        }

        /**
         * Reads the column name indexes if present. If the
         * indexes are based on time then skip over them.
         *
         * @param cfName
         * @return
         */
        private int handleColumnNameIndexes(String cfName, List<IndexHelper.ColumnIndexInfo> columnIndexList) throws IOException
        {
            /* check if we have an index */
            boolean hasColumnIndexes = file_.readBoolean();
            int totalBytesRead = 1;
            /* if we do then deserialize the index */
            if (hasColumnIndexes)
            {
                String tableName = getTableName();
                /* read the index */
                totalBytesRead += IndexHelper.deserializeIndex(tableName, cfName, file_, columnIndexList);
            }
            return totalBytesRead;
        }

        /**
         * Reads the column name indexes if present. If the
         * indexes are based on time then skip over them.
         *
         * @param cfName
         * @return
         */
        private int handleColumnTimeIndexes(String cfName, List<IndexHelper.ColumnIndexInfo> columnIndexList) throws IOException
        {
            /* check if we have an index */
            boolean hasColumnIndexes = file_.readBoolean();
            int totalBytesRead = 1;
            /* if we do then deserialize the index */
            if (hasColumnIndexes)
            {
                if (DatabaseDescriptor.isTimeSortingEnabled(null, cfName))
                {
                    /* read the index */
                    totalBytesRead += IndexHelper.deserializeIndex(getTableName(), cfName, file_, columnIndexList);
                }
                else
                {
                    totalBytesRead += IndexHelper.skipIndex(file_);
                }
            }
            return totalBytesRead;
        }

        /**
         * This method dumps the next key/value into the DataOuputStream
         * passed in. Always use this method to query for application
         * specific data as it will have indexes.
         *
         * @param key       key we are interested in.
         * @param bufOut    DataOutputStream that needs to be filled.
         * @param columnFamilyName name of the columnFamily
         * @param columnNames columnNames we are interested in
         */
        public long next(String key, DataOutputBuffer bufOut, String columnFamilyName, SortedSet<String> columnNames, long position) throws IOException
        {
            assert columnNames != null;

            long bytesRead = -1L;
            if (isEOF() || seekTo(position) < 0)
                return bytesRead;

            /* note the position where the key starts */
            long startPosition = file_.getFilePointer();
            String keyInDisk = file_.readUTF();
            if (keyInDisk != null)
            {
                /*
                 * If key on disk is greater than requested key
                 * we can bail out since we exploit the property
                 * of the SSTable format.
                */
                if (keyInDisk.compareTo(key) > 0)
                    return bytesRead;

                /*
                 * If we found the key then we populate the buffer that
                 * is passed in. If not then we skip over this key and
                 * position ourselves to read the next one.
                */
                if (keyInDisk.equals(key))
                {
                    readColumns(key, bufOut, columnFamilyName, columnNames);
                }
                else
                {
                    /* skip over data portion */
                    int dataSize = file_.readInt();
                    file_.seek(dataSize + file_.getFilePointer());
                }

                long endPosition = file_.getFilePointer();
                bytesRead = endPosition - startPosition;
            }

            return bytesRead;
        }

        private void readColumns(String key, DataOutputBuffer bufOut, String columnFamilyName, SortedSet<String> cNames)
                throws IOException
        {
            int dataSize = file_.readInt();

            /* write the key into buffer */
            bufOut.writeUTF(key);

            /* if we need to read the all the columns do not read the column indexes */
            if (cNames == null || cNames.size() == 0)
            {
                int bytesSkipped = IndexHelper.skipBloomFilterAndIndex(file_);
                /*
                       * read the correct number of bytes for the column family and
                       * write data into buffer
                      */
                dataSize -= bytesSkipped;
                /* write the data size */
                bufOut.writeInt(dataSize);
                /* write the data into buffer, except the boolean we have read */
                bufOut.write(file_, dataSize);
            }
            else
            {
                /* Read the bloom filter summarizing the columns */
                long preBfPos = file_.getFilePointer();
                BloomFilter bf = defreezeBloomFilter();
                long postBfPos = file_.getFilePointer();
                dataSize -= (postBfPos - preBfPos);

                List<IndexHelper.ColumnIndexInfo> columnIndexList = new ArrayList<IndexHelper.ColumnIndexInfo>();
                /* read the column name indexes if present */
                int totalBytesRead = handleColumnNameIndexes(columnFamilyName, columnIndexList);
                dataSize -= totalBytesRead;

                /* read the column family name */
                String cfName = file_.readUTF();
                dataSize -= (utfPrefix_ + cfName.length());

                String cfType = file_.readUTF();
                dataSize -= (utfPrefix_ + cfType.length());

                int indexType = file_.readInt();
                dataSize -= 4;

                /* read local deletion time */
                int localDeletionTime = file_.readInt();
                dataSize -=4;

                /* read if this cf is marked for delete */
                long markedForDeleteAt = file_.readLong();
                dataSize -= 8;

                /* read the total number of columns */
                int totalNumCols = file_.readInt();
                dataSize -= 4;

                // TODO: this is name sorted - but eventually this should be sorted by the same criteria as the col index
                /* get the various column ranges we have to read */
                List<IndexHelper.ColumnRange> columnRanges = IndexHelper.getMultiColumnRangesFromNameIndex(cNames, columnIndexList, dataSize, totalNumCols);

                /* calculate the data size */
                int numColsReturned = 0;
                int dataSizeReturned = 0;
                for (IndexHelper.ColumnRange columnRange : columnRanges)
                {
                    numColsReturned += columnRange.count();
                    Coordinate coordinate = columnRange.coordinate();
                    dataSizeReturned += coordinate.end_ - coordinate.start_;
                }

                // returned data size
                bufOut.writeInt(dataSizeReturned + utfPrefix_ * 2 + cfName.length() + cfType.length() + 4 + 4 + 8 + 4);
                // echo back the CF data we read
                bufOut.writeUTF(cfName);
                bufOut.writeUTF(cfType);
                bufOut.writeInt(indexType);
                bufOut.writeInt(localDeletionTime);
                bufOut.writeLong(markedForDeleteAt);
                /* write number of columns */
                bufOut.writeInt(numColsReturned);
                int prevPosition = 0;
                /* now write all the columns we are required to write */
                for (IndexHelper.ColumnRange columnRange : columnRanges)
                {
                    /* seek to the correct offset to the data */
                    Coordinate coordinate = columnRange.coordinate();
                    file_.skipBytes((int) (coordinate.start_ - prevPosition));
                    bufOut.write(file_, (int) (coordinate.end_ - coordinate.start_));
                    prevPosition = (int) coordinate.end_;
                }
            }
        }

        /**
         * This method dumps the next key/value into the DataOuputStream
         * passed in.
         *
         * @param bufOut DataOutputStream that needs to be filled.
         * @return total number of bytes read/considered
         */
        public long next(DataOutputBuffer bufOut) throws IOException
        {
            long bytesRead = -1L;
            if (isEOF())
                return bytesRead;

            long startPosition = file_.getFilePointer();
            String key = file_.readUTF();
            if (key != null)
            {
                /* write the key into buffer */
                bufOut.writeUTF(key);
                int dataSize = file_.readInt();
                /* write data size into buffer */
                bufOut.writeInt(dataSize);
                /* write the data into buffer */
                bufOut.write(file_, dataSize);
                long endPosition = file_.getFilePointer();
                bytesRead = endPosition - startPosition;
            }

            /*
             * If we have read the bloom filter in the data
             * file we know we are at the end of the file
             * and no further key processing is required. So
             * we return -1 indicating we are at the end of
             * the file.
            */
            if (key.equals(SequenceFile.marker_))
                bytesRead = -1L;
            return bytesRead;
        }
    }

    public static class Reader extends AbstractReader
    {
        Reader(String filename) throws IOException
        {
            super(filename);
            init(filename);
        }

        protected void init(String filename) throws IOException
        {
            file_ = new RandomAccessFile(filename, "r");
        }

        public long getEOF() throws IOException
        {
            return file_.length();
        }

        public long getCurrentPosition() throws IOException
        {
            return file_.getFilePointer();
        }

        public boolean isHealthyFileDescriptor() throws IOException
        {
            return file_.getFD().valid();
        }

        public void seek(long position) throws IOException
        {
            file_.seek(position);
        }

        public boolean isEOF() throws IOException
        {
            return (getCurrentPosition() == getEOF());
        }

        /**
         * Be extremely careful while using this API. This currently
         * used to read the commit log header from the commit logs.
         * Treat this as an internal API.
         *
         * @param bytes read from the buffer into the this array
         */
        public void readDirect(byte[] bytes) throws IOException
        {
            file_.readFully(bytes);
        }

        public long readLong() throws IOException
        {
            return file_.readLong();
        }

        public void close() throws IOException
        {
            file_.close();
        }
    }

    public static class BufferReader extends Reader
    {
        private int size_;

        BufferReader(String filename, int size) throws IOException
        {
            super(filename);
            size_ = size;
        }

        protected void init(String filename) throws IOException
        {
            file_ = new BufferedRandomAccessFile(filename, "r", size_);
        }
    }

    private static Logger logger_ = Logger.getLogger(SequenceFile.class);
    public static final short utfPrefix_ = 2;
    public static final String marker_ = "Bloom-Filter";

    public static IFileWriter writer(String filename) throws IOException
    {
        return new Writer(filename);
    }

    public static IFileWriter bufferedWriter(String filename, int size) throws IOException
    {
        return new BufferWriter(filename, size);
    }

    public static IFileReader reader(String filename) throws IOException
    {
        return new Reader(filename);
    }

    public static IFileReader bufferedReader(String filename, int size) throws IOException
    {
        return new BufferReader(filename, size);
    }
}
