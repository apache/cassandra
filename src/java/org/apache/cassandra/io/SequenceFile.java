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
import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.db.marshal.AbstractType;

import org.apache.log4j.Logger;
import org.apache.commons.lang.ArrayUtils;

/**
 * This class writes key/value pairs sequentially to disk. It is
 * also used to read sequentially from disk. However one could
 * jump to random positions to read data from the file. This class
 * also has many implementations of the IFileWriter and IFileReader
 * interfaces which are exposed through factory methods.
 */

public class SequenceFile
{
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
        private AbstractType comparator_;
        private String subComparatorName_;
        private boolean isAscending_;

        private List<IndexHelper.ColumnIndexInfo> columnIndexList_;
        private long columnStartPosition_;
        private int curRangeIndex_;
        private int allColumnsSize_;
        private int localDeletionTime_;
        private long markedForDeleteAt_;

        public ColumnGroupReader(String filename, String key, String cfName, AbstractType comparator, byte[] startColumn, boolean isAscending, long position) throws IOException
        {
            super(filename, 128 * 1024);
            this.cfName_ = cfName;
            this.comparator_ = comparator;
            this.subComparatorName_ = DatabaseDescriptor.getSubComparator(SSTableReader.parseTableName(filename), cfName).getClass().getCanonicalName();
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
                return Arrays.asList(new IndexHelper.ColumnIndexInfo(ArrayUtils.EMPTY_BYTE_ARRAY, 0, totalNumCols, comparator_));
            }

            List<IndexHelper.ColumnIndexInfo> fullColIndexList = new ArrayList<IndexHelper.ColumnIndexInfo>();
            int accumulatededCols = 0;
            for (IndexHelper.ColumnIndexInfo colPosInfo : columnIndexList)
                accumulatededCols += colPosInfo.count();
            int remainingCols = totalNumCols - accumulatededCols;

            fullColIndexList.add(new IndexHelper.ColumnIndexInfo(ArrayUtils.EMPTY_BYTE_ARRAY, 0, columnIndexList.get(0).count(), comparator_));
            for (int i = 0; i < columnIndexList.size() - 1; i++)
            {
                IndexHelper.ColumnIndexInfo colPosInfo = columnIndexList.get(i);
                fullColIndexList.add(new IndexHelper.ColumnIndexInfo(colPosInfo.name(),
                                                                     colPosInfo.position(),
                                                                     columnIndexList.get(i + 1).count(),
                                                                     comparator_));
            }
            byte[] columnName = columnIndexList.get(columnIndexList.size() - 1).name();
            fullColIndexList.add(new IndexHelper.ColumnIndexInfo(columnName,
                                                                 columnIndexList.get(columnIndexList.size() - 1).position(),
                                                                 remainingCols,
                                                                 comparator_));
            return fullColIndexList;
        }

        private void init(byte[] startColumn, long position) throws IOException
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
                String comparatorName = file_.readUTF();
                assert comparatorName.equals(comparator_.getClass().getCanonicalName());
                String subComparatorName = file_.readUTF(); // subcomparator
                localDeletionTime_ = file_.readInt();
                markedForDeleteAt_ = file_.readLong();
                int totalNumCols = file_.readInt();
                allColumnsSize_ = dataSize - (totalBytesRead + 4 * utfPrefix_ + cfName.length() + cfType_.length() + comparatorName.length() + subComparatorName.length() + 4 + 8 + 4);

                columnStartPosition_ = file_.getFilePointer();
                columnIndexList_ = getFullColumnIndexList(colIndexList, totalNumCols);

                if (startColumn.length == 0 && !isAscending_)
                {
                    /* in this case, we assume that we want to scan from the largest column in descending order. */
                    curRangeIndex_ = columnIndexList_.size() - 1;
                }
                else
                {
                    int index = Collections.binarySearch(columnIndexList_, new IndexHelper.ColumnIndexInfo(startColumn, 0, 0, comparator_));
                    curRangeIndex_ = index < 0 ? (++index) * (-1) - 1 : index;
                }
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
            bufOut.writeUTF(comparator_.getClass().getCanonicalName());
            bufOut.writeUTF(subComparatorName_);
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
         * This method dumps the next key/value into the DataOuputStream
         * passed in. Always use this method to query for application
         * specific data as it will have indexes.
         *
         * @param key       key we are interested in.
         * @param bufOut    DataOutputStream that needs to be filled.
         * @param columnFamilyName name of the columnFamily
         * @param columnNames columnNames we are interested in
         */
        public long next(String key, DataOutputBuffer bufOut, String columnFamilyName, SortedSet<byte[]> columnNames, long position) throws IOException
        {
            assert columnNames != null;

            long bytesRead = -1L;
            if (isEOF() || seekTo(position) < 0)
                return bytesRead;

            /* note the position where the key starts */
            long startPosition = file_.getFilePointer();
            String keyInDisk = file_.readUTF();
            assert keyInDisk.equals(key);
            readColumns(key, bufOut, columnFamilyName, columnNames);

            long endPosition = file_.getFilePointer();
            bytesRead = endPosition - startPosition;

            return bytesRead;
        }

        private void readColumns(String key, DataOutputBuffer bufOut, String columnFamilyName, SortedSet<byte[]> cNames)
        throws IOException
        {
            int dataSize = file_.readInt();

            /* write the key into buffer */
            bufOut.writeUTF(key);

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

            String comparatorName = file_.readUTF();
            dataSize -= (utfPrefix_ + comparatorName.length());

            String subComparatorName = file_.readUTF();
            dataSize -= (utfPrefix_ + subComparatorName.length());

            /* read local deletion time */
            int localDeletionTime = file_.readInt();
            dataSize -=4;

            /* read if this cf is marked for delete */
            long markedForDeleteAt = file_.readLong();
            dataSize -= 8;

            /* read the total number of columns */
            int totalNumCols = file_.readInt();
            dataSize -= 4;

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
            bufOut.writeInt(dataSizeReturned + utfPrefix_ * 4 + cfName.length() + cfType.length() + comparatorName.length() + subComparatorName.length() + 4 + 4 + 8 + 4);
            // echo back the CF data we read
            bufOut.writeUTF(cfName);
            bufOut.writeUTF(cfType);
            bufOut.writeUTF(comparatorName);
            bufOut.writeUTF(subComparatorName);
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

        public void seek(long position) throws IOException
        {
            file_.seek(position);
        }

        public boolean isEOF() throws IOException
        {
            return (getCurrentPosition() == getEOF());
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

    public static AbstractWriter bufferedWriter(String filename, int size) throws IOException
    {
        return new AbstractWriter.BufferWriter(filename, size);
    }

    public static IFileReader bufferedReader(String filename, int size) throws IOException
    {
        return new BufferReader(filename, size);
    }
}
