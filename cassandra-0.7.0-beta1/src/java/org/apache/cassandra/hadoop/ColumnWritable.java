package org.apache.cassandra.hadoop;

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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.io.WritableComparable;

/**
 * The <code>ColumnWritable</code> is a {@link WritableComparable} that denotes
 * a column name and value.
 */
public class ColumnWritable implements WritableComparable<ColumnWritable>
{
    // A comparator that checks if two byte arrays are the same or not.
    public static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR = new Comparator<byte[]>()
    {
        public int compare(byte[] o1, byte[] o2)
        {
            return FBUtilities.compareByteArrays(o1, o2);
        }
    };
    
    // The name and value of the column this writable denotes.
    private byte[] name, value;
    
    public ColumnWritable(byte[] name, byte[] value)
    {
        setName(name);
        setValue(value);
    }
    
    public byte[] getValue()
    {
        return value;
    }
    
    public void setValue(byte[] value)
    {
        this.value = value;
    }
    
    public byte[] getName()
    {
        return name;
    }
    
    public void setName(byte[] name)
    {
        this.name = name;
    }
    
    public void readFields(DataInput in) throws IOException
    {
        name = FBUtilities.readByteArray(in);
        value = FBUtilities.readByteArray(in);
    }
    
    public void write(DataOutput out) throws IOException
    {
        FBUtilities.writeByteArray(name, out);
        FBUtilities.writeByteArray(value, out);
    }
    
    /** Returns true iff <code>o</code> is a ColumnWritable with the same value. */
    public boolean equals(Object o)
    {
        if (!(o instanceof ColumnWritable))
            return false;
        ColumnWritable that = (ColumnWritable) o;
        return compareTo(that) == 0;
    }
    
    public int hashCode()
    {
        return name.hashCode() + value.hashCode();
    }
    
    /** Compares two ColumnWritables. */
    public int compareTo(ColumnWritable o)
    {
        ColumnWritable that = (ColumnWritable) o;
        int nameComparison = BYTE_ARRAY_COMPARATOR.compare(this.name, that.name);
        if (nameComparison != 0)
            return nameComparison;
        return BYTE_ARRAY_COMPARATOR.compare(this.value, that.value);
    }
    
    public String toString()
    {
        return "{ " + name.toString() + " : " + value.toString() + " }";
    }
}
