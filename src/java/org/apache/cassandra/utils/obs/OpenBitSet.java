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
package org.apache.cassandra.utils.obs;

import java.util.Arrays;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * <p>
 * An "open" BitSet implementation that allows direct access to the arrays of words
 * storing the bits.  Derived from Lucene's OpenBitSet, but with a paged backing array
 * (see bits delaration, below).
 * </p>
 * <p>
 * Unlike java.util.bitset, the fact that bits are packed into an array of longs
 * is part of the interface.  This allows efficient implementation of other algorithms
 * by someone other than the author.  It also allows one to efficiently implement
 * alternate serialization or interchange formats.
 * </p>
 * <p>
 * <code>OpenBitSet</code> is faster than <code>java.util.BitSet</code> in most operations
 * and *much* faster at calculating cardinality of sets and results of set operations.
 * It can also handle sets of larger cardinality (up to 64 * 2**32-1)
 * </p>
 * <p>
 * The goals of <code>OpenBitSet</code> are the fastest implementation possible, and
 * maximum code reuse.  Extra safety and encapsulation
 * may always be built on top, but if that's built in, the cost can never be removed (and
 * hence people re-implement their own version in order to get better performance).
 * If you want a "safe", totally encapsulated (and slower and limited) BitSet
 * class, use <code>java.util.BitSet</code>.
 * </p>
 */

public class OpenBitSet implements IBitSet
{
  /**
   * We break the bitset up into multiple arrays to avoid promotion failure caused by attempting to allocate
   * large, contiguous arrays (CASSANDRA-2466).  All sub-arrays but the last are uniformly PAGE_SIZE words;
   * to avoid waste in small bloom filters (of which Cassandra has many: one per row) the last sub-array
   * is sized to exactly the remaining number of words required to achieve the desired set size (CASSANDRA-3618).
   */
  private final long[][] bits;
  private int wlen; // number of words (elements) used in the array
  private final int pageCount;
  private static final int PAGE_SIZE = 4096;

  /**
   * Constructs an OpenBitSet large enough to hold numBits.
   * @param numBits
   */
  public OpenBitSet(long numBits)
  {
      wlen = (int) bits2words(numBits);
      int lastPageSize = wlen % PAGE_SIZE;
      int fullPageCount = wlen / PAGE_SIZE;
      pageCount = fullPageCount + (lastPageSize == 0 ? 0 : 1);

      bits = new long[pageCount][];

      for (int i = 0; i < fullPageCount; ++i)
          bits[i] = new long[PAGE_SIZE];

      if (lastPageSize != 0)
          bits[bits.length - 1] = new long[lastPageSize];
  }

  public OpenBitSet()
  {
    this(64);
  }

  /**
   * @return the pageSize
   */
  public int getPageSize()
  {
      return PAGE_SIZE;
  }

  public int getPageCount()
  {
      return pageCount;
  }

  public long[] getPage(int pageIdx)
  {
      return bits[pageIdx];
  }

  /** Returns the current capacity in bits (1 greater than the index of the last bit) */
  public long capacity() { return ((long)wlen) << 6; }

  @Override
  public long offHeapSize()
  {
      return 0;
  }

    public void addTo(Ref.IdentityCollection identities)
    {
    }

    /**
  * Returns the current capacity of this set.  Included for
  * compatibility.  This is *not* equal to {@link #cardinality}
  */
  public long size()
  {
      return capacity();
  }

  // @Override -- not until Java 1.6
  public long length()
  {
    return capacity();
  }

  /** Returns true if there are no set bits */
  public boolean isEmpty() { return cardinality()==0; }


  /** Expert: gets the number of longs in the array that are in use */
  public int getNumWords() { return wlen; }


  /**
   * Returns true or false for the specified bit index.
   * The index should be less than the OpenBitSet size
   */
  public boolean get(int index)
  {
    int i = index >> 6;               // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    int bit = index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    // TODO perfectionist one can implement this using bit operations
    return (bits[i / PAGE_SIZE][i % PAGE_SIZE ] & bitmask) != 0;
  }

  /**
   * Returns true or false for the specified bit index.
   * The index should be less than the OpenBitSet size.
   */
  public boolean get(long index)
  {
    int i = (int)(index >> 6);               // div 64
    int bit = (int)index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    // TODO perfectionist one can implement this using bit operations
    return (bits[i / PAGE_SIZE][i % PAGE_SIZE ] & bitmask) != 0;
  }

  /**
   * Sets the bit at the specified index.
   * The index should be less than the OpenBitSet size.
   */
  public void set(long index)
  {
    int wordNum = (int)(index >> 6);
    int bit = (int)index & 0x3f;
    long bitmask = 1L << bit;
    bits[ wordNum / PAGE_SIZE ][ wordNum % PAGE_SIZE ] |= bitmask;
  }

  /**
   * Sets the bit at the specified index.
   * The index should be less than the OpenBitSet size.
   */
  public void set(int index)
  {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[ wordNum / PAGE_SIZE ][ wordNum % PAGE_SIZE ] |= bitmask;
  }

  /**
   * clears a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void clear(int index)
  {
    int wordNum = index >> 6;
    int bit = index & 0x03f;
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] &= ~bitmask;
    // hmmm, it takes one more instruction to clear than it does to set... any
    // way to work around this?  If there were only 63 bits per word, we could
    // use a right shift of 10111111...111 in binary to position the 0 in the
    // correct place (using sign extension).
    // Could also use Long.rotateRight() or rotateLeft() *if* they were converted
    // by the JVM into a native instruction.
    // bits[word] &= Long.rotateLeft(0xfffffffe,bit);
  }

  /**
   * clears a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void clear(long index)
  {
    int wordNum = (int)(index >> 6); // div 64
    int bit = (int)index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] &= ~bitmask;
  }

  /**
   * Clears a range of bits.  Clearing past the end does not change the size of the set.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   */
  public void clear(int startIndex, int endIndex)
  {
    if (endIndex <= startIndex) return;

    int startWord = (startIndex>>6);
    if (startWord >= wlen) return;

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord   = ((endIndex-1)>>6);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord)
    {
      bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] &= (startmask | endmask);
      return;
    }


    bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE]  &= startmask;

    int middle = Math.min(wlen, endWord);
    if (startWord / PAGE_SIZE == middle / PAGE_SIZE)
    {
        Arrays.fill(bits[startWord/PAGE_SIZE], (startWord+1) % PAGE_SIZE, middle % PAGE_SIZE, 0L);
    } else
    {
        while (++startWord<middle)
            bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] = 0L;
    }
    if (endWord < wlen)
    {
      bits[endWord / PAGE_SIZE][endWord % PAGE_SIZE] &= endmask;
    }
  }


  /** Clears a range of bits.  Clearing past the end does not change the size of the set.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   */
  public void clear(long startIndex, long endIndex)
  {
    if (endIndex <= startIndex) return;

    int startWord = (int)(startIndex>>6);
    if (startWord >= wlen) return;

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord   = (int)((endIndex-1)>>6);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord)
{
        bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] &= (startmask | endmask);
        return;
    }

    bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE]  &= startmask;

    int middle = Math.min(wlen, endWord);
    if (startWord / PAGE_SIZE == middle / PAGE_SIZE)
    {
        Arrays.fill(bits[startWord/PAGE_SIZE], (startWord+1) % PAGE_SIZE, middle % PAGE_SIZE, 0L);
    } else
    {
        while (++startWord<middle)
            bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] = 0L;
    }
    if (endWord < wlen)
    {
        bits[endWord / PAGE_SIZE][endWord % PAGE_SIZE] &= endmask;
    }
  }

  /** @return the number of set bits */
  public long cardinality()
  {
    long bitCount = 0L;
    for (int i=getPageCount();i-->0;)
        bitCount+=BitUtil.pop_array(bits[i],0,wlen);

    return bitCount;
  }

  /** this = this AND other */
  public void intersect(OpenBitSet other)
  {
    int newLen= Math.min(this.wlen,other.wlen);
    long[][] thisArr = this.bits;
    long[][] otherArr = other.bits;
    int thisPageSize = PAGE_SIZE;
    int otherPageSize = OpenBitSet.PAGE_SIZE;
    // testing against zero can be more efficient
    int pos=newLen;
    while(--pos>=0)
    {
      thisArr[pos / thisPageSize][ pos % thisPageSize] &= otherArr[pos / otherPageSize][pos % otherPageSize];
    }

    if (this.wlen > newLen)
    {
      // fill zeros from the new shorter length to the old length
      for (pos=wlen;pos-->newLen;)
          thisArr[pos / thisPageSize][ pos % thisPageSize] =0;
    }
    this.wlen = newLen;
  }

  // some BitSet compatability methods

  //** see {@link intersect} */
  public void and(OpenBitSet other)
  {
    intersect(other);
  }

  /** Lowers numWords, the number of words in use,
   * by checking for trailing zero words.
   */
  public void trimTrailingZeros()
  {
    int idx = wlen-1;
    while (idx>=0 && bits[idx / PAGE_SIZE][idx % PAGE_SIZE]==0) idx--;
    wlen = idx+1;
  }

  /** returns the number of 64 bit words it would take to hold numBits */
  public static long bits2words(long numBits)
  {
   return (((numBits-1)>>>6)+1);
  }

  /** returns true if both sets have the same bits set */
  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (!(o instanceof OpenBitSet)) return false;
    OpenBitSet a;
    OpenBitSet b = (OpenBitSet)o;
    // make a the larger set.
    if (b.wlen > this.wlen)
    {
      a = b; b=this;
    }
    else
    {
      a=this;
    }

    int aPageSize = OpenBitSet.PAGE_SIZE;
    int bPageSize = OpenBitSet.PAGE_SIZE;

    // check for any set bits out of the range of b
    for (int i=a.wlen-1; i>=b.wlen; i--)
    {
      if (a.bits[i/aPageSize][i % aPageSize]!=0) return false;
    }

    for (int i=b.wlen-1; i>=0; i--)
    {
      if (a.bits[i/aPageSize][i % aPageSize] != b.bits[i/bPageSize][i % bPageSize]) return false;
    }

    return true;
  }


  @Override
  public int hashCode()
  {
    // Start with a zero hash and use a mix that results in zero if the input is zero.
    // This effectively truncates trailing zeros without an explicit check.
    long h = 0;
    for (int i = wlen; --i>=0;)
    {
      h ^= bits[i / PAGE_SIZE][i % PAGE_SIZE];
      h = (h << 1) | (h >>> 63); // rotate left
    }
    // fold leftmost bits into right and add a constant to prevent
    // empty sets from returning 0, which is too common.
    return (int)((h>>32) ^ h) + 0x98761234;
  }

  public void close()
  {
    // noop, let GC do the cleanup.
  }

  public void serialize(DataOutput out) throws IOException
  {
    int bitLength = getNumWords();
    int pageSize = getPageSize();
    int pageCount = getPageCount();

    out.writeInt(bitLength);
    for (int p = 0; p < pageCount; p++)
    {
      long[] bits = getPage(p);
      for (int i = 0; i < pageSize && bitLength-- > 0; i++)
      {
        out.writeLong(bits[i]);
      }
    }
}

  public long serializedSize()
  {
    int bitLength = getNumWords();
    int pageSize = getPageSize();
    int pageCount = getPageCount();

    long size = TypeSizes.sizeof(bitLength); // length
    for (int p = 0; p < pageCount; p++)
    {
      long[] bits = getPage(p);
      for (int i = 0; i < pageSize && bitLength-- > 0; i++)
        size += TypeSizes.sizeof(bits[i]); // bucket
    }
    return size;
  }

  public void clear()
  {
    clear(0, capacity());
  }

  public static OpenBitSet deserialize(DataInput in) throws IOException
  {
    long bitLength = in.readInt();

    OpenBitSet bs = new OpenBitSet(bitLength << 6);
    int pageSize = bs.getPageSize();
    int pageCount = bs.getPageCount();

    for (int p = 0; p < pageCount; p++)
    {
      long[] bits = bs.getPage(p);
      for (int i = 0; i < pageSize && bitLength-- > 0; i++)
        bits[i] = in.readLong();
    }
    return bs;
  }
}
