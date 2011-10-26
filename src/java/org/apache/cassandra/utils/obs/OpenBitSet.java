/**
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

package org.apache.cassandra.utils.obs;

import java.util.Arrays;
import java.io.Serializable;
import java.util.BitSet;

/** An "open" BitSet implementation that allows direct access to the array of words
 * storing the bits.
 * <p/>
 * Unlike java.util.bitset, the fact that bits are packed into an array of longs
 * is part of the interface.  This allows efficient implementation of other algorithms
 * by someone other than the author.  It also allows one to efficiently implement
 * alternate serialization or interchange formats.
 * <p/>
 * <code>OpenBitSet</code> is faster than <code>java.util.BitSet</code> in most operations
 * and *much* faster at calculating cardinality of sets and results of set operations.
 * It can also handle sets of larger cardinality (up to 64 * 2**32-1)
 * <p/>
 * The goals of <code>OpenBitSet</code> are the fastest implementation possible, and
 * maximum code reuse.  Extra safety and encapsulation
 * may always be built on top, but if that's built in, the cost can never be removed (and
 * hence people re-implement their own version in order to get better performance).
 * If you want a "safe", totally encapsulated (and slower and limited) BitSet
 * class, use <code>java.util.BitSet</code>.
 * <p/>
 * <h3>Performance Results</h3>
 *
 Test system: Pentium 4, Sun Java 1.5_06 -server -Xbatch -Xmx64M
<br/>BitSet size = 1,000,000
<br/>Results are java.util.BitSet time divided by OpenBitSet time.
<table border="1">
 <tr>
  <th></th> <th>cardinality</th> <th>intersect_count</th> <th>union</th> <th>nextSetBit</th> <th>get</th> <th>iterator</th>
 </tr>
 <tr>
  <th>50% full</th> <td>3.36</td> <td>3.96</td> <td>1.44</td> <td>1.46</td> <td>1.99</td> <td>1.58</td>
 </tr>
 <tr>
   <th>1% full</th> <td>3.31</td> <td>3.90</td> <td>&nbsp;</td> <td>1.04</td> <td>&nbsp;</td> <td>0.99</td>
 </tr>
</table>
<br/>
Test system: AMD Opteron, 64 bit linux, Sun Java 1.5_06 -server -Xbatch -Xmx64M
<br/>BitSet size = 1,000,000
<br/>Results are java.util.BitSet time divided by OpenBitSet time.
<table border="1">
 <tr>
  <th></th> <th>cardinality</th> <th>intersect_count</th> <th>union</th> <th>nextSetBit</th> <th>get</th> <th>iterator</th>
 </tr>
 <tr>
  <th>50% full</th> <td>2.50</td> <td>3.50</td> <td>1.00</td> <td>1.03</td> <td>1.12</td> <td>1.25</td>
 </tr>
 <tr>
   <th>1% full</th> <td>2.51</td> <td>3.49</td> <td>&nbsp;</td> <td>1.00</td> <td>&nbsp;</td> <td>1.02</td>
 </tr>
</table>
 */

public class OpenBitSet implements Serializable {
  protected long[][] bits;
  protected int wlen;   // number of words (elements) used in the array
  /**
   * length of bits[][] page in long[] elements. 
   * Choosing unform size for all sizes of bitsets fight fragmentation for very large
   * bloom filters.
   */
  protected static final int PAGE_SIZE= 4096; 

  /** Constructs an OpenBitSet large enough to hold numBits.
   *
   * @param numBits
   */
  public OpenBitSet(long numBits) 
  {
      this(numBits,true);
  }
  
  public OpenBitSet(long numBits, boolean allocatePages) 
  {
    wlen= bits2words(numBits);    
    
    bits = new long[getPageCount()][];
    
    if (allocatePages)
    {
        for (int allocated=0,i=0;allocated<wlen;allocated+=PAGE_SIZE,i++)
            bits[i]=new long[PAGE_SIZE];
    }
  }

  public OpenBitSet() {
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
      return wlen / PAGE_SIZE + 1;
  }

  public long[] getPage(int pageIdx)
  {
      return bits[pageIdx];
  }
  
  /** Contructs an OpenBitset from a BitSet
  */
  public OpenBitSet(BitSet bits) {
    this(bits.length());
  }

  /** Returns the current capacity in bits (1 greater than the index of the last bit) */
  public long capacity() { return ((long)wlen) << 6; }

 /**
  * Returns the current capacity of this set.  Included for
  * compatibility.  This is *not* equal to {@link #cardinality}
  */
  public long size() {
      return capacity();
  }

  // @Override -- not until Java 1.6
  public long length() {
    return capacity();
  }

  /** Returns true if there are no set bits */
  public boolean isEmpty() { return cardinality()==0; }


  /** Expert: gets the number of longs in the array that are in use */
  public int getNumWords() { return wlen; }


  /** Returns true or false for the specified bit index. */
  public boolean get(int index) {
    int i = index >> 6;               // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    if (i>=wlen) return false;

    int bit = index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    // TODO perfectionist one can implement this using bit operations
    return (bits[i / PAGE_SIZE ][i % PAGE_SIZE] & bitmask) != 0;
  }


 /** Returns true or false for the specified bit index.
   * The index should be less than the OpenBitSet size
   */
  public boolean fastGet(int index) {
    int i = index >> 6;               // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    int bit = index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    // TODO perfectionist one can implement this using bit operations
    return (bits[i / PAGE_SIZE][i % PAGE_SIZE ] & bitmask) != 0;
  }



 /** Returns true or false for the specified bit index
  */
  public boolean get(long index) {
    int i = (int)(index >> 6);             // div 64
    if (i>=wlen) return false;
    int bit = (int)index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    // TODO perfectionist one can implement this using bit operations
    return (bits[i / PAGE_SIZE][i % PAGE_SIZE ] & bitmask) != 0;
  }

  /** Returns true or false for the specified bit index.
   * The index should be less than the OpenBitSet size.
   */
  public boolean fastGet(long index) {
    int i = (int)(index >> 6);               // div 64
    int bit = (int)index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    // TODO perfectionist one can implement this using bit operations
    return (bits[i / PAGE_SIZE][i % PAGE_SIZE ] & bitmask) != 0;
  }

  /** returns 1 if the bit is set, 0 if not.
   * The index should be less than the OpenBitSet size
   */
  public int getBit(int index) {
    int i = index >> 6;                // div 64
    int bit = index & 0x3f;            // mod 64
    return ((int)(bits[i / PAGE_SIZE][i % PAGE_SIZE ]>>>bit)) & 0x01;
  }


  /** sets a bit, expanding the set size if necessary */
  public void set(long index) {
    int wordNum = expandingWordNum(index);
    int bit = (int)index & 0x3f;
    long bitmask = 1L << bit;
    bits[ wordNum / PAGE_SIZE ][ wordNum % PAGE_SIZE ] |= bitmask;
  }


 /** Sets the bit at the specified index.
  * The index should be less than the OpenBitSet size.
  */
  public void fastSet(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[ wordNum / PAGE_SIZE ][ wordNum % PAGE_SIZE ] |= bitmask;
  }

 /** Sets the bit at the specified index.
  * The index should be less than the OpenBitSet size.
  */
  public void fastSet(long index) {
    int wordNum = (int)(index >> 6);
    int bit = (int)index & 0x3f;
    long bitmask = 1L << bit;
    bits[ wordNum / PAGE_SIZE ][ wordNum % PAGE_SIZE ] |= bitmask;
  }

  /** Sets a range of bits, expanding the set size if necessary
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to set
   */
  public void set(long startIndex, long endIndex) {
    if (endIndex <= startIndex) return;

    int startWord = (int)(startIndex>>6);

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord   = expandingWordNum(endIndex-1);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    if (startWord == endWord) {
      bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] |= (startmask & endmask);
      return;
    }

    assert startWord / PAGE_SIZE == endWord / PAGE_SIZE : "cross page sets not suppotred at all - they are not used";

    bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] |= startmask;
    Arrays.fill(bits[ startWord / PAGE_SIZE], (startWord+1) % PAGE_SIZE , endWord % PAGE_SIZE , -1L);
    bits[endWord / PAGE_SIZE][endWord % PAGE_SIZE] |= endmask;
  }



  protected int expandingWordNum(long index) {
    int wordNum = (int)(index >> 6);
    if (wordNum>=wlen) {
      ensureCapacity(index+1);
      wlen = wordNum+1;
    }
    return wordNum;
  }


  /** clears a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void fastClear(int index) {
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

  /** clears a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void fastClear(long index) {
    int wordNum = (int)(index >> 6); // div 64
    int bit = (int)index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] &= ~bitmask;
  }

  /** clears a bit, allowing access beyond the current set size without changing the size.*/
  public void clear(long index) {
    int wordNum = (int)(index >> 6); // div 64
    if (wordNum>=wlen) return;
    int bit = (int)index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] &= ~bitmask;
  }

  /** Clears a range of bits.  Clearing past the end does not change the size of the set.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   */
  public void clear(int startIndex, int endIndex) {
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

    if (startWord == endWord) {
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
    if (endWord < wlen) {
      bits[endWord / PAGE_SIZE][endWord % PAGE_SIZE] &= endmask;
    }
  }


  /** Clears a range of bits.  Clearing past the end does not change the size of the set.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   */
  public void clear(long startIndex, long endIndex) {
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

    if (startWord == endWord) {
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
    if (endWord < wlen) {
        bits[endWord / PAGE_SIZE][endWord % PAGE_SIZE] &= endmask;
    }
  }



  /** Sets a bit and returns the previous value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean getAndSet(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    boolean val = (bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] & bitmask) != 0;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] |= bitmask;
    return val;
  }

  /** Sets a bit and returns the previous value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean getAndSet(long index) {
    int wordNum = (int)(index >> 6);      // div 64
    int bit = (int)index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    boolean val = (bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] & bitmask) != 0;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] |= bitmask;
    return val;
  }

  /** flips a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void fastFlip(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] ^= bitmask;
  }

  /** flips a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void fastFlip(long index) {
    int wordNum = (int)(index >> 6);   // div 64
    int bit = (int)index & 0x3f;       // mod 64
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] ^= bitmask;
  }

  /** flips a bit, expanding the set size if necessary */
  public void flip(long index) {
    int wordNum = expandingWordNum(index);
    int bit = (int)index & 0x3f;       // mod 64
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] ^= bitmask;
  }

  /** flips a bit and returns the resulting bit value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean flipAndGet(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] ^= bitmask;
    return (bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] & bitmask) != 0;
  }

  /** flips a bit and returns the resulting bit value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean flipAndGet(long index) {
    int wordNum = (int)(index >> 6);   // div 64
    int bit = (int)index & 0x3f;       // mod 64
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] ^= bitmask;
    return (bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] & bitmask) != 0;
  }

  /** Flips a range of bits, expanding the set size if necessary
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to flip
   */
  public void flip(long startIndex, long endIndex) {
    if (endIndex <= startIndex) return;
    int startWord = (int)(startIndex>>6);

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord   = expandingWordNum(endIndex-1);

    /*** Grrr, java shifting wraps around so -1L>>>64 == -1
     * for that reason, make sure not to use endmask if the bits to flip will
     * be zero in the last word (redefine endWord to be the last changed...)
    long startmask = -1L << (startIndex & 0x3f);     // example: 11111...111000
    long endmask = -1L >>> (64-(endIndex & 0x3f));   // example: 00111...111111
    ***/

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    if (startWord == endWord) {
      bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] ^= (startmask & endmask);
      return;
    }

    bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] ^= startmask;

    for (int i=startWord+1; i<endWord; i++) {
      bits[i / PAGE_SIZE][ i % PAGE_SIZE] = ~bits[i / PAGE_SIZE][ i % PAGE_SIZE];
    }

    bits[endWord / PAGE_SIZE][endWord % PAGE_SIZE] ^= endmask;
  }


  /** @return the number of set bits */
  public long cardinality() 
  {
    long bitCount = 0L;
    for (int i=getPageCount();i-->0;)
        bitCount+=BitUtil.pop_array(bits[i],0,wlen);
    
    return bitCount;
  }

  /** Returns the index of the first set bit starting at the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public int nextSetBit(int index) {
    int i = index>>6;
    if (i>=wlen) return -1;
    int subIndex = index & 0x3f;      // index within the word
    long word = bits[i / PAGE_SIZE][ i % PAGE_SIZE] >> subIndex;  // skip all the bits to the right of index

    if (word!=0) {
      return (i<<6) + subIndex + BitUtil.ntz(word);
    }

    while(++i < wlen) {
      word = bits[i / PAGE_SIZE][i % PAGE_SIZE];
      if (word!=0) return (i<<6) + BitUtil.ntz(word);
    }

    return -1;
  }

  /** Returns the index of the first set bit starting at the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public long nextSetBit(long index) {
    int i = (int)(index>>>6);
    if (i>=wlen) return -1;
    int subIndex = (int)index & 0x3f; // index within the word
    long word = bits[i / PAGE_SIZE][i % PAGE_SIZE] >>> subIndex;  // skip all the bits to the right of index

    if (word!=0) {
      return (((long)i)<<6) + (subIndex + BitUtil.ntz(word));
    }

    while(++i < wlen) {
      word = bits[i / PAGE_SIZE][i % PAGE_SIZE];
      if (word!=0) return (((long)i)<<6) + BitUtil.ntz(word);
    }

    return -1;
  }

  /** this = this AND other */
  public void intersect(OpenBitSet other) {
    int newLen= Math.min(this.wlen,other.wlen);
    long[][] thisArr = this.bits;
    long[][] otherArr = other.bits;
    int thisPageSize = this.PAGE_SIZE;
    int otherPageSize = other.PAGE_SIZE;
    // testing against zero can be more efficient
    int pos=newLen;
    while(--pos>=0) {
      thisArr[pos / thisPageSize][ pos % thisPageSize] &= otherArr[pos / otherPageSize][pos % otherPageSize];
    }
    
    if (this.wlen > newLen) {
      // fill zeros from the new shorter length to the old length
      for (pos=wlen;pos-->newLen;)
          thisArr[pos / thisPageSize][ pos % thisPageSize] =0;
    }
    this.wlen = newLen;
  }

  // some BitSet compatability methods

  //** see {@link intersect} */
  public void and(OpenBitSet other) {
    intersect(other);
  }


  /** Expand the long[] with the size given as a number of words (64 bit longs).
   * getNumWords() is unchanged by this call.
   */
  public void ensureCapacityWords(int numWords) 
  {
    assert numWords<=wlen : "Growing of paged bitset is not supported"; 
  }

  /** Ensure that the long[] is big enough to hold numBits, expanding it if necessary.
   * getNumWords() is unchanged by this call.
   */
  public void ensureCapacity(long numBits) {
    ensureCapacityWords(bits2words(numBits));
  }

  /** Lowers numWords, the number of words in use,
   * by checking for trailing zero words.
   */
  public void trimTrailingZeros() {
    int idx = wlen-1;
    while (idx>=0 && bits[idx / PAGE_SIZE][idx % PAGE_SIZE]==0) idx--;
    wlen = idx+1;
  }

  /** returns the number of 64 bit words it would take to hold numBits */
  public static int bits2words(long numBits) {
   return (int)(((numBits-1)>>>6)+1);
  }

  /** returns true if both sets have the same bits set */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof OpenBitSet)) return false;
    OpenBitSet a;
    OpenBitSet b = (OpenBitSet)o;
    // make a the larger set.
    if (b.wlen > this.wlen) {
      a = b; b=this;
    } else {
      a=this;
    }
    
    int aPageSize = this.PAGE_SIZE;
    int bPageSize = b.PAGE_SIZE;

    // check for any set bits out of the range of b
    for (int i=a.wlen-1; i>=b.wlen; i--) {
      if (a.bits[i/aPageSize][i % aPageSize]!=0) return false;
    }

    for (int i=b.wlen-1; i>=0; i--) {
      if (a.bits[i/aPageSize][i % aPageSize] != b.bits[i/bPageSize][i % bPageSize]) return false;
    }

    return true;
  }


  @Override
  public int hashCode() {
    // Start with a zero hash and use a mix that results in zero if the input is zero.
    // This effectively truncates trailing zeros without an explicit check.
    long h = 0;
    for (int i = wlen; --i>=0;) {
      h ^= bits[i / PAGE_SIZE][i % PAGE_SIZE];
      h = (h << 1) | (h >>> 63); // rotate left
    }
    // fold leftmost bits into right and add a constant to prevent
    // empty sets from returning 0, which is too common.
    return (int)((h>>32) ^ h) + 0x98761234;
  }

}


