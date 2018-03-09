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

/**  A variety of high efficiency bit twiddling routines.
 * @lucene.internal
 */
final class BitUtil
{

  /** Returns the number of bits set in the long */
  public static int pop(long x)
  {
  /* Hacker's Delight 32 bit pop function:
   * http://www.hackersdelight.org/HDcode/newCode/pop_arrayHS.cc
   *
  int pop(unsigned x) {
     x = x - ((x >> 1) & 0x55555555);
     x = (x & 0x33333333) + ((x >> 2) & 0x33333333);
     x = (x + (x >> 4)) & 0x0F0F0F0F;
     x = x + (x >> 8);
     x = x + (x >> 16);
     return x & 0x0000003F;
    }
  ***/

    // 64 bit java version of the C function from above
    x = x - ((x >>> 1) & 0x5555555555555555L);
    x = (x & 0x3333333333333333L) + ((x >>>2 ) & 0x3333333333333333L);
    x = (x + (x >>> 4)) & 0x0F0F0F0F0F0F0F0FL;
    x = x + (x >>> 8);
    x = x + (x >>> 16);
    x = x + (x >>> 32);
    return ((int)x) & 0x7F;
  }

  /*** Returns the number of set bits in an array of longs. */
  public static long pop_array(long A[], int wordOffset, int numWords)
  {
    /*
    * Robert Harley and David Seal's bit counting algorithm, as documented
    * in the revisions of Hacker's Delight
    * http://www.hackersdelight.org/revisions.pdf
    * http://www.hackersdelight.org/HDcode/newCode/pop_arrayHS.cc
    *
    * This function was adapted to Java, and extended to use 64 bit words.
    * if only we had access to wider registers like SSE from java...
    *
    * This function can be transformed to compute the popcount of other functions
    * on bitsets via something like this:
    * sed 's/A\[\([^]]*\)\]/\(A[\1] \& B[\1]\)/g'
    *
    */
    int n = wordOffset+numWords;
    long tot=0, tot8=0;
    long ones=0, twos=0, fours=0;

    int i;
    for (i = wordOffset; i <= n - 8; i+=8)
    {
      /***  C macro from Hacker's Delight
       #define CSA(h,l, a,b,c) \
       {unsigned u = a ^ b; unsigned v = c; \
       h = (a & b) | (u & v); l = u ^ v;}
       ***/

      long twosA,twosB,foursA,foursB,eights;

      // CSA(twosA, ones, ones, A[i], A[i+1])
      {
        long b=A[i], c=A[i+1];
        long u=ones ^ b;
        twosA=(ones & b)|( u & c);
        ones=u^c;
      }
      // CSA(twosB, ones, ones, A[i+2], A[i+3])
      {
        long b=A[i+2], c=A[i+3];
        long u=ones^b;
        twosB =(ones&b)|(u&c);
        ones=u^c;
      }
      //CSA(foursA, twos, twos, twosA, twosB)
      {
        long u=twos^twosA;
        foursA=(twos&twosA)|(u&twosB);
        twos=u^twosB;
      }
      //CSA(twosA, ones, ones, A[i+4], A[i+5])
      {
        long b=A[i+4], c=A[i+5];
        long u=ones^b;
        twosA=(ones&b)|(u&c);
        ones=u^c;
      }
      // CSA(twosB, ones, ones, A[i+6], A[i+7])
      {
        long b=A[i+6], c=A[i+7];
        long u=ones^b;
        twosB=(ones&b)|(u&c);
        ones=u^c;
      }
      //CSA(foursB, twos, twos, twosA, twosB)
      {
        long u=twos^twosA;
        foursB=(twos&twosA)|(u&twosB);
        twos=u^twosB;
      }

      //CSA(eights, fours, fours, foursA, foursB)
      {
        long u=fours^foursA;
        eights=(fours&foursA)|(u&foursB);
        fours=u^foursB;
      }
      tot8 += pop(eights);
    }

    // handle trailing words in a binary-search manner...
    // derived from the loop above by setting specific elements to 0.
    // the original method in Hackers Delight used a simple for loop:
    //   for (i = i; i < n; i++)      // Add in the last elements
    //  tot = tot + pop(A[i]);

    if (i<=n-4)
    {
      long twosA, twosB, foursA, eights;
      {
        long b=A[i], c=A[i+1];
        long u=ones ^ b;
        twosA=(ones & b)|( u & c);
        ones=u^c;
      }
      {
        long b=A[i+2], c=A[i+3];
        long u=ones^b;
        twosB =(ones&b)|(u&c);
        ones=u^c;
      }
      {
        long u=twos^twosA;
        foursA=(twos&twosA)|(u&twosB);
        twos=u^twosB;
      }
      eights=fours&foursA;
      fours=fours^foursA;

      tot8 += pop(eights);
      i+=4;
    }

    if (i<=n-2)
    {
      long b=A[i], c=A[i+1];
      long u=ones ^ b;
      long twosA=(ones & b)|( u & c);
      ones=u^c;

      long foursA=twos&twosA;
      twos=twos^twosA;

      long eights=fours&foursA;
      fours=fours^foursA;

      tot8 += pop(eights);
      i+=2;
    }

    if (i<n)
    {
      tot += pop(A[i]);
    }

    tot += (pop(fours)<<2)
            + (pop(twos)<<1)
            + pop(ones)
            + (tot8<<3);

    return tot;
  }

  /** Returns the popcount or cardinality of the two sets after an intersection.
   * Neither array is modified.
   */
  public static long pop_intersect(long A[], long B[], int wordOffset, int numWords)
  {
    // generated from pop_array via sed 's/A\[\([^]]*\)\]/\(A[\1] \& B[\1]\)/g'
    int n = wordOffset+numWords;
    long tot=0, tot8=0;
    long ones=0, twos=0, fours=0;

    int i;
    for (i = wordOffset; i <= n - 8; i+=8)
    {
      long twosA,twosB,foursA,foursB,eights;

      // CSA(twosA, ones, ones, (A[i] & B[i]), (A[i+1] & B[i+1]))
      {
        long b=(A[i] & B[i]), c=(A[i+1] & B[i+1]);
        long u=ones ^ b;
        twosA=(ones & b)|( u & c);
        ones=u^c;
      }
      // CSA(twosB, ones, ones, (A[i+2] & B[i+2]), (A[i+3] & B[i+3]))
      {
        long b=(A[i+2] & B[i+2]), c=(A[i+3] & B[i+3]);
        long u=ones^b;
        twosB =(ones&b)|(u&c);
        ones=u^c;
      }
      //CSA(foursA, twos, twos, twosA, twosB)
      {
        long u=twos^twosA;
        foursA=(twos&twosA)|(u&twosB);
        twos=u^twosB;
      }
      //CSA(twosA, ones, ones, (A[i+4] & B[i+4]), (A[i+5] & B[i+5]))
      {
        long b=(A[i+4] & B[i+4]), c=(A[i+5] & B[i+5]);
        long u=ones^b;
        twosA=(ones&b)|(u&c);
        ones=u^c;
      }
      // CSA(twosB, ones, ones, (A[i+6] & B[i+6]), (A[i+7] & B[i+7]))
      {
        long b=(A[i+6] & B[i+6]), c=(A[i+7] & B[i+7]);
        long u=ones^b;
        twosB=(ones&b)|(u&c);
        ones=u^c;
      }
      //CSA(foursB, twos, twos, twosA, twosB)
      {
        long u=twos^twosA;
        foursB=(twos&twosA)|(u&twosB);
        twos=u^twosB;
      }

      //CSA(eights, fours, fours, foursA, foursB)
      {
        long u=fours^foursA;
        eights=(fours&foursA)|(u&foursB);
        fours=u^foursB;
      }
      tot8 += pop(eights);
    }


    if (i<=n-4)
    {
      long twosA, twosB, foursA, eights;
      {
        long b=(A[i] & B[i]), c=(A[i+1] & B[i+1]);
        long u=ones ^ b;
        twosA=(ones & b)|( u & c);
        ones=u^c;
      }
      {
        long b=(A[i+2] & B[i+2]), c=(A[i+3] & B[i+3]);
        long u=ones^b;
        twosB =(ones&b)|(u&c);
        ones=u^c;
      }
      {
        long u=twos^twosA;
        foursA=(twos&twosA)|(u&twosB);
        twos=u^twosB;
      }
      eights=fours&foursA;
      fours=fours^foursA;

      tot8 += pop(eights);
      i+=4;
    }

    if (i<=n-2)
    {
      long b=(A[i] & B[i]), c=(A[i+1] & B[i+1]);
      long u=ones ^ b;
      long twosA=(ones & b)|( u & c);
      ones=u^c;

      long foursA=twos&twosA;
      twos=twos^twosA;

      long eights=fours&foursA;
      fours=fours^foursA;

      tot8 += pop(eights);
      i+=2;
    }

    if (i<n)
    {
      tot += pop((A[i] & B[i]));
    }

    tot += (pop(fours)<<2)
            + (pop(twos)<<1)
            + pop(ones)
            + (tot8<<3);

    return tot;
  }

  /** Returns the popcount or cardinality of the union of two sets.
    * Neither array is modified.
    */
   public static long pop_union(long A[], long B[], int wordOffset, int numWords)
   {
     // generated from pop_array via sed 's/A\[\([^]]*\)\]/\(A[\1] \| B[\1]\)/g'
     int n = wordOffset+numWords;
     long tot=0, tot8=0;
     long ones=0, twos=0, fours=0;

     int i;
     for (i = wordOffset; i <= n - 8; i+=8)
     {
       /***  C macro from Hacker's Delight
        #define CSA(h,l, a,b,c) \
        {unsigned u = a ^ b; unsigned v = c; \
        h = (a & b) | (u & v); l = u ^ v;}
        ***/

       long twosA,twosB,foursA,foursB,eights;

       // CSA(twosA, ones, ones, (A[i] | B[i]), (A[i+1] | B[i+1]))
       {
         long b=(A[i] | B[i]), c=(A[i+1] | B[i+1]);
         long u=ones ^ b;
         twosA=(ones & b)|( u & c);
         ones=u^c;
       }
       // CSA(twosB, ones, ones, (A[i+2] | B[i+2]), (A[i+3] | B[i+3]))
       {
         long b=(A[i+2] | B[i+2]), c=(A[i+3] | B[i+3]);
         long u=ones^b;
         twosB =(ones&b)|(u&c);
         ones=u^c;
       }
       //CSA(foursA, twos, twos, twosA, twosB)
       {
         long u=twos^twosA;
         foursA=(twos&twosA)|(u&twosB);
         twos=u^twosB;
       }
       //CSA(twosA, ones, ones, (A[i+4] | B[i+4]), (A[i+5] | B[i+5]))
       {
         long b=(A[i+4] | B[i+4]), c=(A[i+5] | B[i+5]);
         long u=ones^b;
         twosA=(ones&b)|(u&c);
         ones=u^c;
       }
       // CSA(twosB, ones, ones, (A[i+6] | B[i+6]), (A[i+7] | B[i+7]))
       {
         long b=(A[i+6] | B[i+6]), c=(A[i+7] | B[i+7]);
         long u=ones^b;
         twosB=(ones&b)|(u&c);
         ones=u^c;
       }
       //CSA(foursB, twos, twos, twosA, twosB)
       {
         long u=twos^twosA;
         foursB=(twos&twosA)|(u&twosB);
         twos=u^twosB;
       }

       //CSA(eights, fours, fours, foursA, foursB)
       {
         long u=fours^foursA;
         eights=(fours&foursA)|(u&foursB);
         fours=u^foursB;
       }
       tot8 += pop(eights);
     }


     if (i<=n-4)
     {
       long twosA, twosB, foursA, eights;
       {
         long b=(A[i] | B[i]), c=(A[i+1] | B[i+1]);
         long u=ones ^ b;
         twosA=(ones & b)|( u & c);
         ones=u^c;
       }
       {
         long b=(A[i+2] | B[i+2]), c=(A[i+3] | B[i+3]);
         long u=ones^b;
         twosB =(ones&b)|(u&c);
         ones=u^c;
       }
       {
         long u=twos^twosA;
         foursA=(twos&twosA)|(u&twosB);
         twos=u^twosB;
       }
       eights=fours&foursA;
       fours=fours^foursA;

       tot8 += pop(eights);
       i+=4;
     }

     if (i<=n-2)
     {
       long b=(A[i] | B[i]), c=(A[i+1] | B[i+1]);
       long u=ones ^ b;
       long twosA=(ones & b)|( u & c);
       ones=u^c;

       long foursA=twos&twosA;
       twos=twos^twosA;

       long eights=fours&foursA;
       fours=fours^foursA;

       tot8 += pop(eights);
       i+=2;
     }

     if (i<n)
     {
       tot += pop((A[i] | B[i]));
     }

     tot += (pop(fours)<<2)
             + (pop(twos)<<1)
             + pop(ones)
             + (tot8<<3);

     return tot;
   }

  /** Returns the popcount or cardinality of A & ~B
   * Neither array is modified.
   */
  public static long pop_andnot(long A[], long B[], int wordOffset, int numWords)
  {
    // generated from pop_array via sed 's/A\[\([^]]*\)\]/\(A[\1] \& ~B[\1]\)/g'
    int n = wordOffset+numWords;
    long tot=0, tot8=0;
    long ones=0, twos=0, fours=0;

    int i;
    for (i = wordOffset; i <= n - 8; i+=8)
    {
      /***  C macro from Hacker's Delight
       #define CSA(h,l, a,b,c) \
       {unsigned u = a ^ b; unsigned v = c; \
       h = (a & b) | (u & v); l = u ^ v;}
       ***/

      long twosA,twosB,foursA,foursB,eights;

      // CSA(twosA, ones, ones, (A[i] & ~B[i]), (A[i+1] & ~B[i+1]))
      {
        long b=(A[i] & ~B[i]), c=(A[i+1] & ~B[i+1]);
        long u=ones ^ b;
        twosA=(ones & b)|( u & c);
        ones=u^c;
      }
      // CSA(twosB, ones, ones, (A[i+2] & ~B[i+2]), (A[i+3] & ~B[i+3]))
      {
        long b=(A[i+2] & ~B[i+2]), c=(A[i+3] & ~B[i+3]);
        long u=ones^b;
        twosB =(ones&b)|(u&c);
        ones=u^c;
      }
      //CSA(foursA, twos, twos, twosA, twosB)
      {
        long u=twos^twosA;
        foursA=(twos&twosA)|(u&twosB);
        twos=u^twosB;
      }
      //CSA(twosA, ones, ones, (A[i+4] & ~B[i+4]), (A[i+5] & ~B[i+5]))
      {
        long b=(A[i+4] & ~B[i+4]), c=(A[i+5] & ~B[i+5]);
        long u=ones^b;
        twosA=(ones&b)|(u&c);
        ones=u^c;
      }
      // CSA(twosB, ones, ones, (A[i+6] & ~B[i+6]), (A[i+7] & ~B[i+7]))
      {
        long b=(A[i+6] & ~B[i+6]), c=(A[i+7] & ~B[i+7]);
        long u=ones^b;
        twosB=(ones&b)|(u&c);
        ones=u^c;
      }
      //CSA(foursB, twos, twos, twosA, twosB)
      {
        long u=twos^twosA;
        foursB=(twos&twosA)|(u&twosB);
        twos=u^twosB;
      }

      //CSA(eights, fours, fours, foursA, foursB)
      {
        long u=fours^foursA;
        eights=(fours&foursA)|(u&foursB);
        fours=u^foursB;
      }
      tot8 += pop(eights);
    }


    if (i<=n-4)
    {
      long twosA, twosB, foursA, eights;
      {
        long b=(A[i] & ~B[i]), c=(A[i+1] & ~B[i+1]);
        long u=ones ^ b;
        twosA=(ones & b)|( u & c);
        ones=u^c;
      }
      {
        long b=(A[i+2] & ~B[i+2]), c=(A[i+3] & ~B[i+3]);
        long u=ones^b;
        twosB =(ones&b)|(u&c);
        ones=u^c;
      }
      {
        long u=twos^twosA;
        foursA=(twos&twosA)|(u&twosB);
        twos=u^twosB;
      }
      eights=fours&foursA;
      fours=fours^foursA;

      tot8 += pop(eights);
      i+=4;
    }

    if (i<=n-2)
    {
      long b=(A[i] & ~B[i]), c=(A[i+1] & ~B[i+1]);
      long u=ones ^ b;
      long twosA=(ones & b)|( u & c);
      ones=u^c;

      long foursA=twos&twosA;
      twos=twos^twosA;

      long eights=fours&foursA;
      fours=fours^foursA;

      tot8 += pop(eights);
      i+=2;
    }

    if (i<n)
    {
      tot += pop((A[i] & ~B[i]));
    }

    tot += (pop(fours)<<2)
            + (pop(twos)<<1)
            + pop(ones)
            + (tot8<<3);

    return tot;
  }

  public static long pop_xor(long A[], long B[], int wordOffset, int numWords)
  {
    int n = wordOffset+numWords;
    long tot=0, tot8=0;
    long ones=0, twos=0, fours=0;

    int i;
    for (i = wordOffset; i <= n - 8; i+=8)
    {
      /***  C macro from Hacker's Delight
       #define CSA(h,l, a,b,c) \
       {unsigned u = a ^ b; unsigned v = c; \
       h = (a & b) | (u & v); l = u ^ v;}
       ***/

      long twosA,twosB,foursA,foursB,eights;

      // CSA(twosA, ones, ones, (A[i] ^ B[i]), (A[i+1] ^ B[i+1]))
      {
        long b=(A[i] ^ B[i]), c=(A[i+1] ^ B[i+1]);
        long u=ones ^ b;
        twosA=(ones & b)|( u & c);
        ones=u^c;
      }
      // CSA(twosB, ones, ones, (A[i+2] ^ B[i+2]), (A[i+3] ^ B[i+3]))
      {
        long b=(A[i+2] ^ B[i+2]), c=(A[i+3] ^ B[i+3]);
        long u=ones^b;
        twosB =(ones&b)|(u&c);
        ones=u^c;
      }
      //CSA(foursA, twos, twos, twosA, twosB)
      {
        long u=twos^twosA;
        foursA=(twos&twosA)|(u&twosB);
        twos=u^twosB;
      }
      //CSA(twosA, ones, ones, (A[i+4] ^ B[i+4]), (A[i+5] ^ B[i+5]))
      {
        long b=(A[i+4] ^ B[i+4]), c=(A[i+5] ^ B[i+5]);
        long u=ones^b;
        twosA=(ones&b)|(u&c);
        ones=u^c;
      }
      // CSA(twosB, ones, ones, (A[i+6] ^ B[i+6]), (A[i+7] ^ B[i+7]))
      {
        long b=(A[i+6] ^ B[i+6]), c=(A[i+7] ^ B[i+7]);
        long u=ones^b;
        twosB=(ones&b)|(u&c);
        ones=u^c;
      }
      //CSA(foursB, twos, twos, twosA, twosB)
      {
        long u=twos^twosA;
        foursB=(twos&twosA)|(u&twosB);
        twos=u^twosB;
      }

      //CSA(eights, fours, fours, foursA, foursB)
      {
        long u=fours^foursA;
        eights=(fours&foursA)|(u&foursB);
        fours=u^foursB;
      }
      tot8 += pop(eights);
    }


    if (i<=n-4)
    {
      long twosA, twosB, foursA, eights;
      {
        long b=(A[i] ^ B[i]), c=(A[i+1] ^ B[i+1]);
        long u=ones ^ b;
        twosA=(ones & b)|( u & c);
        ones=u^c;
      }
      {
        long b=(A[i+2] ^ B[i+2]), c=(A[i+3] ^ B[i+3]);
        long u=ones^b;
        twosB =(ones&b)|(u&c);
        ones=u^c;
      }
      {
        long u=twos^twosA;
        foursA=(twos&twosA)|(u&twosB);
        twos=u^twosB;
      }
      eights=fours&foursA;
      fours=fours^foursA;

      tot8 += pop(eights);
      i+=4;
    }

    if (i<=n-2)
    {
      long b=(A[i] ^ B[i]), c=(A[i+1] ^ B[i+1]);
      long u=ones ^ b;
      long twosA=(ones & b)|( u & c);
      ones=u^c;

      long foursA=twos&twosA;
      twos=twos^twosA;

      long eights=fours&foursA;
      fours=fours^foursA;

      tot8 += pop(eights);
      i+=2;
    }

    if (i<n)
    {
      tot += pop((A[i] ^ B[i]));
    }

    tot += (pop(fours)<<2)
            + (pop(twos)<<1)
            + pop(ones)
            + (tot8<<3);

    return tot;
  }

  /* python code to generate ntzTable
  def ntz(val):
    if val==0: return 8
    i=0
    while (val&0x01)==0:
      i = i+1
      val >>= 1
    return i
  print ','.join([ str(ntz(i)) for i in range(256) ])
  ***/
  /** keyspaceName of number of trailing zeros in a byte */
  public static final byte[] ntzTable = {8,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,6,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,7,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,6,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0};


  /** Returns number of trailing zeros in a 64 bit long value. */
  public static int ntz(long val)
  {
    // A full binary search to determine the low byte was slower than
    // a linear search for nextSetBit().  This is most likely because
    // the implementation of nextSetBit() shifts bits to the right, increasing
    // the probability that the first non-zero byte is in the rhs.
    //
    // This implementation does a single binary search at the top level only
    // so that all other bit shifting can be done on ints instead of longs to
    // remain friendly to 32 bit architectures.  In addition, the case of a
    // non-zero first byte is checked for first because it is the most common
    // in dense bit arrays.

    int lower = (int)val;
    int lowByte = lower & 0xff;
    if (lowByte != 0) return ntzTable[lowByte];

    if (lower!=0)
    {
      lowByte = (lower>>>8) & 0xff;
      if (lowByte != 0) return ntzTable[lowByte] + 8;
      lowByte = (lower>>>16) & 0xff;
      if (lowByte != 0) return ntzTable[lowByte] + 16;
      // no need to mask off low byte for the last byte in the 32 bit word
      // no need to check for zero on the last byte either.
      return ntzTable[lower>>>24] + 24;
    }
    else
    {
      // grab upper 32 bits
      int upper=(int)(val>>32);
      lowByte = upper & 0xff;
      if (lowByte != 0) return ntzTable[lowByte] + 32;
      lowByte = (upper>>>8) & 0xff;
      if (lowByte != 0) return ntzTable[lowByte] + 40;
      lowByte = (upper>>>16) & 0xff;
      if (lowByte != 0) return ntzTable[lowByte] + 48;
      // no need to mask off low byte for the last byte in the 32 bit word
      // no need to check for zero on the last byte either.
      return ntzTable[upper>>>24] + 56;
    }
  }

  /** Returns number of trailing zeros in a 32 bit int value. */
  public static int ntz(int val)
  {
    // This implementation does a single binary search at the top level only.
    // In addition, the case of a non-zero first byte is checked for first
    // because it is the most common in dense bit arrays.

    int lowByte = val & 0xff;
    if (lowByte != 0) return ntzTable[lowByte];
    lowByte = (val>>>8) & 0xff;
    if (lowByte != 0) return ntzTable[lowByte] + 8;
    lowByte = (val>>>16) & 0xff;
    if (lowByte != 0) return ntzTable[lowByte] + 16;
    // no need to mask off low byte for the last byte.
    // no need to check for zero on the last byte either.
    return ntzTable[val>>>24] + 24;
  }

  /** returns 0 based index of first set bit
   * (only works for x!=0)
   * <br/> This is an alternate implementation of ntz()
   */
  public static int ntz2(long x)
  {
   int n = 0;
   int y = (int)x;
   if (y==0) {n+=32; y = (int)(x>>>32); }   // the only 64 bit shift necessary
   if ((y & 0x0000FFFF) == 0) { n+=16; y>>>=16; }
   if ((y & 0x000000FF) == 0) { n+=8; y>>>=8; }
   return (ntzTable[ y & 0xff ]) + n;
  }

  /** returns 0 based index of first set bit
   * <br/> This is an alternate implementation of ntz()
   */
  public static int ntz3(long x)
  {
   // another implementation taken from Hackers Delight, extended to 64 bits
   // and converted to Java.
   // Many 32 bit ntz algorithms are at http://www.hackersdelight.org/HDcode/ntz.cc
   int n = 1;

   // do the first step as a long, all others as ints.
   int y = (int)x;
   if (y==0) {n+=32; y = (int)(x>>>32); }
   if ((y & 0x0000FFFF) == 0) { n+=16; y>>>=16; }
   if ((y & 0x000000FF) == 0) { n+=8; y>>>=8; }
   if ((y & 0x0000000F) == 0) { n+=4; y>>>=4; }
   if ((y & 0x00000003) == 0) { n+=2; y>>>=2; }
   return n - (y & 1);
  }


  /** returns true if v is a power of two or zero*/
  public static boolean isPowerOfTwo(int v)
  {
    return ((v & (v-1)) == 0);
  }

  /** returns true if v is a power of two or zero*/
  public static boolean isPowerOfTwo(long v)
  {
    return ((v & (v-1)) == 0);
  }

  /** returns the next highest power of two, or the current value if it's already a power of two or zero*/
  public static int nextHighestPowerOfTwo(int v)
  {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
  }

  /** returns the next highest power of two, or the current value if it's already a power of two or zero*/
   public static long nextHighestPowerOfTwo(long v)
   {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
  }

}
