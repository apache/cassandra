<!---
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Byte-comparable translation of types (ByteComparable/ByteSource)

## Problem / Motivation

Cassandra has a very heavy reliance on comparisons — they are used throughout read and write paths, coordination,
compaction, etc. to be able to order and merge results. It also supports a range of types which often require the
compared object to be completely in memory to order correctly, which in turn has necessitated interfaces where
comparisons can only be applied if the compared objects are completely loaded.

This has some rather painful implications on the performance of the database, both in terms of the time it takes to load,
compare and garbage collect, as well as in terms of the space required to hold complete keys in on-disk indices and
deserialized versions in in-memory data structures. In addition to this, the reliance on comparisons forces Cassandra to
use only comparison-based structures, which aren’t the most efficient.

There is no way to escape the need to compare and order objects in Cassandra, but the machinery for doing this can be
done much more smartly if we impose some simple structure in the objects we deal with — byte ordering.

The term “byte order” as used in this document refers to the property of being ordered via lexicographic compare on the
unsigned values of the byte contents. Some of the types in Cassandra already have this property (e.g. strings, blobs),
but other most heavily used ones (e.g. integers, uuids) don’t.

When byte order is universally available for the types used for keys, several key advantages can be put to use:

- Comparisons can be done using a single simple method, core machinery doesn’t need to know anything about types.
- Prefix differences are enough to define order; unique prefixes can be used instead of complete keys.
- Tries can be used to store, query and iterate over ranges of keys, providing fast lookup and prefix compression.
- Merging can be performed by merging tries, significantly reducing the number of necessary comparisons.

## Ordering the types

As we want to keep all existing functionality in Cassandra, we need to be able to deal with existing
non-byte-order-comparable types. This requires some form of conversion of each value to a sequence of bytes that can be
byte-order compared (also called "byte-comparable"), as well as the inverse conversion from byte-comparable to value.

As one of the main advantages of byte order is the ability to decide comparisons early, without having to read the whole
of the input sequence, byte-ordered interpretations of values are represented as sources of bytes with unknown length,
using the interface `ByteSource`. The interface declares one method, `next()` which produces the next byte of the
stream, or `ByteSource.END_OF_STREAM` if the stream is exhausted.

`END_OF_STREAM` is chosen as `-1` (`(int) -1`, which is outside the range of possible byte values), to make comparing
two byte sources as trivial (and thus fast) as possible.

To be able to completely abstract type information away from the storage machinery, we also flatten complex types into
single byte sequences. To do this, we add separator bytes in front, between components, and at the end and do some
encoding of variable-length sequences.

The other interface provided by this package `ByteComparable`, is an entity whose byte-ordered interpretation can be
requested. The interface is implemented by `DecoratedKey`, and can be requested for clustering keys and bounds using
`ClusteringComparator.asByteComparable`. The inverse translation is provided by
`Buffer/NativeDecoratedKey.fromByteComparable` and `ClusteringComparator.clustering/bound/boundaryFromByteComparable`.

The (rather technical) paragraphs below detail the encoding we have chosen for the various types. For simplicity we
only discuss the bidirectional `OSS50` version of the translation. The implementations in code of the various mappings
are in the releavant `AbstractType` subclass.

### Desired properties

Generally, we desire the following two properties from the byte-ordered translations of values we use in the database:

- Comparison equivalence (1):  
    <math xmlns="http://www.w3.org/1998/Math/MathML">
      <semantics>
        <mstyle displaystyle="true">
          <mo>&#x2200;</mo>
          <mi>x</mi>
          <mo>,</mo>
          <mi>y</mi>
          <mo>&#x2208;</mo>
          <mi>T</mi>
          <mo>,</mo>
          <mrow>
            <mtext>compareBytesUnsigned</mtext>
          </mrow>
          <mrow>
            <mo>(</mo>
            <mi>T</mi>
            <mo>.</mo>
            <mrow>
              <mtext>byteOrdered</mtext>
            </mrow>
            <mrow>
              <mo>(</mo>
              <mi>x</mi>
              <mo>)</mo>
            </mrow>
            <mo>,</mo>
            <mi>T</mi>
            <mo>.</mo>
            <mrow>
              <mtext>byteOrdered</mtext>
            </mrow>
            <mrow>
              <mo>(</mo>
              <mi>y</mi>
              <mo>)</mo>
            </mrow>
            <mo>)</mo>
          </mrow>
          <mo>=</mo>
          <mi>T</mi>
          <mo>.</mo>
          <mrow>
            <mtext>compare</mtext>
          </mrow>
          <mrow>
            <mo>(</mo>
            <mi>x</mi>
            <mo>,</mo>
            <mi>y</mi>
            <mo>)</mo>
          </mrow>
        </mstyle>
        <!-- <annotation encoding="text/x-asciimath">forall x,y in T, "compareBytesUnsigned"(T."byteOrdered"(x), T."byteOrdered"(y))=T."compare"(x, y)</annotation> -->
      </semantics>
    </math>
- Prefix-freedom (2):  
    <math xmlns="http://www.w3.org/1998/Math/MathML">
      <semantics>
        <mstyle displaystyle="true">
          <mo>&#x2200;</mo>
          <mi>x</mi>
          <mo>,</mo>
          <mi>y</mi>
          <mo>&#x2208;</mo>
          <mi>T</mi>
          <mo>,</mo>
          <mi>T</mi>
          <mo>.</mo>
          <mrow>
            <mtext>byteOrdered</mtext>
          </mrow>
          <mrow>
            <mo>(</mo>
            <mi>x</mi>
            <mo>)</mo>
          </mrow>
          <mrow>
            <mspace width="1ex" />
            <mtext> is not a prefix of </mtext>
            <mspace width="1ex" />
          </mrow>
          <mi>T</mi>
          <mo>.</mo>
          <mrow>
            <mtext>byteOrdered</mtext>
          </mrow>
          <mrow>
            <mo>(</mo>
            <mi>y</mi>
            <mo>)</mo>
          </mrow>
        </mstyle>
        <!-- <annotation encoding="text/x-asciimath">forall x,y in T, T."byteOrdered"(x) " is not a prefix of " T."byteOrdered"(y)</annotation> -->
      </semantics>
    </math>

The former is the essential requirement, and the latter allows construction of encodings of sequences of multiple
values, as well as a little more efficiency in the data structures.

To more efficiently encode byte-ordered blobs, however, we use a slightly tweaked version of the above requirements:

- Comparison equivalence (3):  
    <math xmlns="http://www.w3.org/1998/Math/MathML">
      <semantics>
        <mstyle displaystyle="true">
          <mo>&#x2200;</mo>
          <mi>x</mi>
          <mo>,</mo>
          <mi>y</mi>
          <mo>&#x2208;</mo>
          <mi>T</mi>
          <mo>,</mo>
          <mo>&#x2200;</mo>
          <msub>
            <mi>b</mi>
            <mn>1</mn>
          </msub>
          <mo>,</mo>
          <msub>
            <mi>b</mi>
            <mn>2</mn>
          </msub>
          <mo>&#x2208;</mo>
          <mrow>
            <mo>[</mo>
            <mn>0x10</mn>
            <mo>-</mo>
            <mn>0xEF</mn>
            <mo>]</mo>
          </mrow>
          <mo>,</mo>
            <mtext><br/></mtext>
          <mrow>
            <mtext>compareBytesUnsigned</mtext>
          </mrow>
          <mrow>
            <mo>(</mo>
            <mi>T</mi>
            <mo>.</mo>
            <mrow>
              <mtext>byteOrdered</mtext>
            </mrow>
            <mrow>
              <mo>(</mo>
              <mi>x</mi>
              <mo>)</mo>
            </mrow>
            <mo>+</mo>
            <msub>
              <mi>b</mi>
              <mn>1</mn>
            </msub>
            <mo>,</mo>
            <mi>T</mi>
            <mo>.</mo>
            <mrow>
              <mtext>byteOrdered</mtext>
            </mrow>
            <mrow>
              <mo>(</mo>
              <mi>y</mi>
              <mo>)</mo>
            </mrow>
            <mo>+</mo>
            <msub>
              <mi>b</mi>
              <mn>2</mn>
            </msub>
            <mo>)</mo>
          </mrow>
          <mo>=</mo>
          <mi>T</mi>
          <mo>.</mo>
          <mrow>
            <mtext>compare</mtext>
          </mrow>
          <mrow>
            <mo>(</mo>
            <mi>x</mi>
            <mo>,</mo>
            <mi>y</mi>
            <mo>)</mo>
          </mrow>
        </mstyle>
        <!-- <annotation encoding="text/x-asciimath">forall x,y in T, forall b_1, b_2 in [0x10-0xEF],
    "compareBytesUnsigned"(T."byteOrdered"(x)+b_1, T."byteOrdered"(y)+b_2)=T."compare"(x, y)</annotation> -->
      </semantics>
    </math>
- Weak prefix-freedom (4):  
    <math xmlns="http://www.w3.org/1998/Math/MathML">
      <semantics>
        <mstyle displaystyle="true">
          <mo>&#x2200;</mo>
          <mi>x</mi>
          <mo>,</mo>
          <mi>y</mi>
          <mo>&#x2208;</mo>
          <mi>T</mi>
          <mo>,</mo>
          <mo>&#x2200;</mo>
          <mi>b</mi>
          <mo>&#x2208;</mo>
          <mrow>
            <mo>[</mo>
            <mn>0x10</mn>
            <mo>-</mo>
            <mn>0xEF</mn>
            <mo>]</mo>
          </mrow>
          <mo>,</mo>
            <mtext><br/></mtext>
          <mrow>
            <mo>(</mo>
            <mi>T</mi>
            <mo>.</mo>
            <mrow>
              <mtext>byteOrdered</mtext>
            </mrow>
            <mrow>
              <mo>(</mo>
              <mi>x</mi>
              <mo>)</mo>
            </mrow>
            <mo>+</mo>
            <mi>b</mi>
            <mo>)</mo>
          </mrow>
          <mrow>
            <mspace width="1ex" />
            <mtext> is not a prefix of </mtext>
            <mspace width="1ex" />
          </mrow>
          <mi>T</mi>
          <mo>.</mo>
          <mrow>
            <mtext>byteOrdered</mtext>
          </mrow>
          <mrow>
            <mo>(</mo>
            <mi>y</mi>
            <mo>)</mo>
          </mrow>
        </mstyle>
        <!-- <annotation encoding="text/x-asciimath">forall x,y in T, forall b in [0x10-0xEF],
    (T."byteOrdered"(x)+b) " is not a prefix of " T."byteOrdered"(y)</annotation> -->
      </semantics>
    </math>

These versions allow the addition of a separator byte after each value, and guarantee that the combination with
separator fulfills the original requirements. (3) is somewhat stronger than (1) but is necessarily true if (2) is also
in force, while (4) trivially follows from (2).

## Fixed length unsigned integers (Murmur token, date/time)

This is the trivial case, as we can simply use the input bytes in big-endian order. The comparison result is the same,
and fixed length values are trivially prefix free, i.e. (1) and (2) are satisfied, and thus (3) and (4) follow from the
observation above.

## Fixed-length signed integers (byte, short, int, legacy bigint)

As above, but we need to invert the sign bit of the number to put negative numbers before positives. This maps
`MIN_VALUE` to `0x00`..., `-1` to `0x7F…`, `0` to `0x80…`, and `MAX_VALUE` to `0xFF…`; comparing the resulting number
as an unsigned integer has the same effect as comparing the source signed.

Examples:


| Type and value | bytes                   | encodes as              |
| -------------- | ----------------------- | ----------------------- |
| int 1          | 00 00 00 01             | 80 00 00 01             |
| short -1       | FF FF                   | 7F FF                   |
| byte 0         | 00                      | 80                      |
| byte -2        | FE                      | 7E                      |
| int MAX_VALUE  | 7F FF FF FF             | FF FF FF FF             |
| long MIN_VALUE | 80 00 00 00 00 00 00 00 | 00 00 00 00 00 00 00 00 |

## Variable-length encoding of integers (current bigint)

Another way to encode integers that may save significant amounts of space when smaller numbers are often in use, but
still permits large values to be efficiently encoded, is to use an encoding scheme similar to UTF-8.

For unsigned numbers this can be done by starting the number with as many 1s in most significant bits as there are
additional bytes in the encoding, followed by a 0, and the bits of the number. Numbers between 0 and 127 are encoded
in one byte, and each additional byte adds 7 more bits. Values that use all 8 bytes do not need a 9th bit of 0 and can
thus fit 9 bytes. Because longer numbers have more 1s in their MSBs, they compare
higher than shorter ones (and we always use the shortest representation). Because the length is specified through these
initial bits, no value can be a prefix of another.


| Value            | bytes                   | encodes as                 |
| ---------------- | ----------------------- | -------------------------- |
| 0                | 00 00 00 00 00 00 00 00 | 00                         |
| 1                | 00 00 00 00 00 00 00 01 | 01                         |
| 127 (2^7-1)      | 00 00 00 00 00 00 00 7F | 7F                         |
| 128 (2^7)        | 00 00 00 00 00 00 00 80 | 80 80                      |
| 16383 (2^14 - 1) | 00 00 00 00 00 00 3F FF | BF FF                      |
| 16384 (2^14)     | 00 00 00 00 00 00 40 00 | C0 40 00                   |
| 2^31 - 1         | 00 00 00 00 7F FF FF FF | F0 7F FF FF FF             |
| 2^31             | 00 00 00 00 80 00 00 00 | F0 80 00 00 00             |
| 2^56 - 1         | 00 FF FF FF FF FF FF FF | FE FF FF FF FF FF FF FF    |
| 2^56             | 01 00 00 00 00 00 00 00 | FF 01 00 00 00 00 00 00 00 |
| 2^64- 1          | FF FF FF FF FF FF FF FF | FF FF FF FF FF FF FF FF FF |

To encode signed numbers, we must start with the sign bit, and must also ensure that longer negative numbers sort
smaller than shorter ones. The first bit of the encoding is the inverted sign (i.e. 1 for positive, 0 for negative),
followed by the length encoded as a sequence of bits that matches the inverted sign, followed by a bit that differs
(like above, not necessary for 9-byte encodings) and the bits of the number's two's complement.


| Value             | bytes                   | encodes as                 |
| ----------------- | ----------------------- | -------------------------- |
| 1                 | 00 00 00 00 00 00 00 01 | 81                         |
| -1                | FF FF FF FF FF FF FF FF | 7F                         |
| 0                 | 00 00 00 00 00 00 00 00 | 80                         |
| 63                | 00 00 00 00 00 00 00 3F | BF                         |
| -64               | FF FF FF FF FF FF FF C0 | 40                         |
| 64                | 00 00 00 00 00 00 00 40 | C0 40                      |
| -65               | FF FF FF FF FF FF FF BF | 3F BF                      |
| 8191              | 00 00 00 00 00 00 1F FF | DF FF                      |
| 8192              | 00 00 00 00 00 00 20 00 | E0 20 00                   |
| Integer.MAX_VALUE | 00 00 00 00 7F FF FF FF | F8 7F FF FF FF             |
| Long.MIN_VALUE    | 80 00 00 00 00 00 00 00 | 00 00 00 00 00 00 00 00 00 |

## Fixed-size floating-point numbers (float, double)

IEEE-754 was designed with byte-by-byte comparisons in mind, and provides an important guarantee about the bytes of a
floating point number:
* If x and y are of the same sign, bytes(x) ≥ bytes(y) ⇔ |x| ≥ |y|.

Thus, to be able to order floating point numbers as unsigned integers, we can:

* Flip the sign bit so negatives are smaller than positive numbers.
* If the number was negative, also flip all the other bits so larger magnitudes become smaller integers.

This matches exactly the behaviour of `Double.compare`, which doesn’t fully agree with numerical comparisons (see spec)
in order to define a natural order over the floating point numbers.

Examples:


| Type and value | bytes                   | encodes as              |
| -------------- | ----------------------- | ----------------------- |
| float +1.0     | 3F 80 00 00             | BF 80 00 00             |
| float +0.0     | 00 00 00 00             | 80 00 00 00             |
| float -0.0     | 80 00 00 00             | 7F FF FF FF             |
| float -1.0     | BF 80 00 00             | 40 7F FF FF             |
| double +1.0    | 3F F0 00 00 00 00 00 00 | BF F0 00 00 00 00 00 00 |
| double +Inf    | 7F F0 00 00 00 00 00 00 | FF F0 00 00 00 00 00 00 |
| double -Inf    | FF F0 00 00 00 00 00 00 | 00 0F FF FF FF FF FF FF |
| double NaN     | 7F F8 00 00 00 00 00 00 | FF F8 00 00 00 00 00 00 |

## UUIDs

UUIDs are fixed-length unsigned integers, where the UUID version/type is compared first, and where bits need to be
reordered for the time UUIDs. To create a byte-ordered representation, we reorder the bytes: pull the version digit
first, then the rest of the digits, using the special time order if the version is equal to one.

Examples:


| Type and value | bytes                                | encodes as                       |
| -------------- | ------------------------------------ | -------------------------------- |
| Random (v4)    | cc520882-9507-44fb-8fc9-b349ecdee658 | 4cc52088295074fb8fc9b349ecdee658 |
| Time (v1)      | 2a92d750-d8dc-11e6-a2de-cf8ecd4cf053 | 11e6d8dc2a92d750a2decf8ecd4cf053 |

## Multi-component sequences (Partition or Clustering keys, tuples), bounds and nulls

As mentioned above, we encode sequences by adding separator bytes in front, between components, and a terminator at the
end. The values we chose for the separator and terminator are `0x40` and `0x38`, and they serve several purposes:

- Permits partially specified bounds, with strict/exclusive or non-strict/inclusive semantics. This is done by finishing
  a bound with a terminator value that is smaller/greater than the separator and terminator. We can use `0x20` for `<`/`≥`
  and `0x60` for `≤`/`>`.
- Permits encoding of `null` and `empty` values. We use `0x3E` as the separator for nulls and `0x3F` for empty,
  followed by no value bytes. This is always smaller than a sequence with non-null value for this component, but not
  smaller than a sequence that ends in this component.
- Helps identify the ending of variable-length components (see below).

Examples:


| Types and values         | bytes                  | encodes as                     |
| ------------------------ | ---------------------- | ------------------------------ |
| (short 1, float 1.0)     | 00 01, 3F 80 00 00     | 40·80 01·40·BF 80 00 00·38 |
| (short -1, null)         | FF FF, —              | 40·7F FF·3E·38              |
| ≥ (short 0, float -Inf) | 00 00, FF 80 00 00, >= | 40·80 00·40·00 7F FF FF·20 |
| < (short MIN)            | 80 00, <=              | 40·00 00·20                  |
| \> (null)                |                        | 3E·60                         |
| BOTTOM                   |                        | 20                             |
| TOP                      |                        | 60                             |

(The middle dot · doesn't exist in the encoding, it’s just a visualisation of the boundaries in the examples.)

Since:

- all separators in use are within `0x10`-`0xEF`, and
- we use the same separator for internal components, with the exception of nulls which we encode with a smaller
  separator
- the sequence has a fixed number of components or we use a different trailing value whenever it can be shorter

the properties (3) and (4) guarantee that the byte comparison of the encoding goes in the same direction as the
lexicographical comparison of the sequence. In combination with the third point above, (4) also ensures that no encoding
is a prefix of another. Since we have (1) and (2), (3) and (4) are also satisfied.

Note that this means that the encodings of all partition and clustering keys used in the database will be prefix-free.

## Variable-length byte comparables (ASCII, UTF-8 strings, blobs, InetAddress)

In isolation, these can be compared directly without reinterpretation. However, once we place these inside a flattened
sequence of values we need to clearly define the boundaries between values while maintaining order. To do this we use an
end-of-value marker; since shorter values must be smaller than longer, this marker must be 0 and we need to find a way
to encode/escape actual 0s in the input sequence.

The method we chose for this is the following:

- If the input does not end on `00`, a `00` byte is appended at the end.
- If the input contains a `00` byte, it is encoded as `00 FF`.
- If the input contains a sequence of *n* `00` bytes, they are encoded as `00` `FE` (*n*-1 times) `FF`
  (so that we don’t double the size of `00` blobs).
- If the input ends in `00`, the last `FF` is changed to `FE`
  (to ensure it’s smaller than the same value with `00` appended).

Examples:


| bytes/sequence     | encodes as               |
| ------------------ | ------------------------ |
| 22 00              | 22 00 FE                 |
| 22 00 00 33        | 22 00 FE FF 33 00        |
| 22 00 11           | 22 00 FF 11 00           |
| (blob 22, short 0) | 40·22 00·40·80 00·40 |
| ≥ (blob 22 00)    | 40·22 00 FE·20         |
| ≤ (blob 22 00 00) | 40·22 00 FE FE·60      |

Within the encoding, a `00` byte can only be followed by a `FE` or `FF` byte, and hence if an encoding is a prefix of
another, the latter has to have a `FE` or `FF` as the next byte, which ensures both (4) (adding `10`-`EF` to the former
makes it no longer a prefix of the latter) and (3) (adding `10`-`EF` to the former makes it smaller than the latter; in
this case the original value of the former is a prefix of the original value of the latter).

## Variable-length integers (varint, RandomPartitioner token), legacy encoding

If integers of unbounded length are guaranteed to start with a non-zero digit, to compare them we can first use a signed
length, as numbers with longer representations have higher magnitudes. Only if the lengths match we need to compare the
sequence of digits, which now has a known length.

(Note: The meaning of “digit” here is not the same as “decimal digit”. We operate with numbers stored as bytes, thus it
makes most sense to treat the numbers as encoded in base-256, where each digit is a byte.)

This translates to the following encoding of varints:

- Strip any leading zeros. Note that for negative numbers, `BigInteger` encodes leading 0 as `0xFF`.
- If the length is 128 or greater, lead with a byte of `0xFF` (positive) or `0x00` (negative) for every 128 until there
  are less than 128 left.
- Encode the sign and (remaining) length of the number as a byte:
  - `0x80 + (length - 1)` for positive numbers (so that greater magnitude is higher);
  - `0x7F - (length - 1)` for negative numbers (so that greater magnitude is lower, and all negatives are lower than
    positives).
- Paste the bytes of the number, 2’s complement encoded for negative numbers (`BigInteger` already applies the 2’s
  complement).

Since when comparing two numbers we either have a difference in the length prefix, or the lengths are the same if we
need to compare the content bytes, there is no risk that a longer number can be confused with a shorter combined in a
multi-component sequence. In other words, no value can be a prefix of another, thus we have (1) and (2) and thus (3) and (4)
as well.

Examples:


|   value | bytes            | encodes as              |
| ------: | ---------------- | ----------------------- |
|       0 | 00               | 80·00                  |
|       1 | 01               | 80·01                  |
|      -1 | FF               | 7F·FF                  |
|     255 | 00 FF            | 80·FF                  |
|    -256 | FF 00            | 7F·00                  |
|     256 | 01 00            | 81·01 00               |
|    2^16 | 01 00 00         | 82·01 00 00            |
|   -2^32 | FF 00 00 00 00   | 7C·00 00 00 00         |
|  2^1024 | 01 00(128 times) | FF 80·01 00(128 times) |
| -2^2048 | FF 00(256 times) | 00 00 80·00(256 times) |

(Middle dot · shows the transition point between length and digits.)

## Variable-length integers, current encoding

Because variable-length integers are also often used to store smaller range integers, it makes sense to also apply
the variable-length integer encoding. Thus, the current varint scheme chooses to:

- Strip any leading zeros. Note that for negative numbers, `BigInteger` encodes leading 0 as `0xFF`.
- Map numbers directly to their [variable-length integer encoding](#variable-length-encoding-of-integers-current-bigint),
  if they have 6 bytes or less.
- Otherwise, encode as:
  - a sign byte (00 for negative numbers, FF for positive, distinct from the leading byte of the variable-length
    encoding above)
  - a variable-length encoded number of bytes adjusted by -7 (so that the smallest length this encoding uses maps to
    0), inverted for negative numbers (so that greater length compares smaller)
  - the bytes of the number, two's complement encoded.
    We never use a longer encoding (e.g. using the second method if variable-length suffices or with added 00 leading
    bytes) if a shorter one suffices.

By the same reasoning as above, and the fact that the sign byte cannot be confused with a variable-length encoding
first byte, no value can be a prefix of another. As the sign byte compares smaller for negative (respectively bigger
for positive numbers) than any variable-length encoded integer, the comparison order is maintained when one number
uses variable-length encoding, and the other doesn't. Longer numbers compare smaller when negative (because of the
inverted length bytes), and bigger when positive.

Examples:


|   value | bytes                   | encodes as                      |
| ------: | ----------------------- | ------------------------------- |
|       0 | 00                      | 80                              |
|       1 | 01                      | 81                              |
|      -1 | FF                      | 7F                              |
|     255 | 00 FF                   | C0 FF                           |
|    -256 | FF 00                   | 3F 00                           |
|     256 | 01 00                   | C1 00                           |
|    2^16 | 01 00 00                | E1 00 00                        |
|   -2^32 | FF 00 00 00 00          | 07 00 00 00 00                  |
|  2^56-1 | 00 FF FF FF FF FF FF FF | FE FF FF FF FF FF FF FF         |
|   -2^56 | FF 00 00 00 00 00 00 00 | 01 00 00 00 00 00 00 00         |
|    2^56 | 01 00 00 00 00 00 00 00 | FF·00·01 00 00 00 00 00 00 00 |
| -2^56-1 | FE FF FF FF FF FF FF FF | 00·FF·FE FF FF FF FF FF FF FF |
|  2^1024 | 01 00(128 times)        | FF·7A·01 00(128 times)        |
| -2^2048 | FF 00(256 times)        | 00·7F 06·00(256 times)        |

(Middle dot · shows the transition point between length and digits.)

## Variable-length floating-point decimals (decimal)

Variable-length floats are more complicated, but we can treat them similarly to IEEE-754 floating point numbers, by
normalizing them by splitting them into sign, mantissa and signed exponent such that the mantissa is a number below 1
with a non-zero leading digit. We can then compare sign, exponent and mantissa in sequence (where the comparison of
exponent and mantissa are with reversed meaning if the sign is negative) and that gives us the decimal ordering.

A bit of extra care must be exercised when encoding decimals. Since fractions like `0.1` cannot be perfectly encoded in
binary, decimals (and mantissas) cannot be encoded in binary or base-256 correctly. A decimal base must be used; since
we deal with bytes, it makes most sense to make things a little more efficient by using base-100. Floating-point
encoding and the comparison idea from the previous paragraph work in any number base.

`BigDecimal` presents a further challenge, as it encodes decimals using a mixture of bases: numbers have a binary-
encoded integer part and a decimal power-of-ten scale. The bytes produced by a `BigDecimal` are thus not suitable for
direct conversion to byte comparable and we must first instantiate the bytes as a `BigDecimal`, and then apply the
class’s methods to operate on it as a number.

We then use the following encoding:

- If the number is 0, the encoding is a single `0x80` byte.
- Convert the input to signed mantissa and signed exponent in base-100. If the value is negative, invert the sign of the
  exponent to form the "modulated exponent".
- Output a byte encoding:
  - the sign of the number encoded as `0x80` if positive and `0x00` if negative,
  - the exponent length (stripping leading 0s) in bytes as `0x40 + modulated_exponent_length`, where the length is given
    with the sign of the modulated exponent.
- Output `exponent_length` bytes of modulated exponent, 2’s complement encoded so that negative values are correctly
  ordered.
- Output `0x80 + leading signed byte of mantissa`, which is obtained by multiplying the mantissa by 100 and rounding to
  -∞. The rounding is done so that the remainder of the mantissa becomes positive, and thus every new byte adds some
  value to it, making shorter sequences lower in value.
- Update the mantissa to be the remainder after the rounding above. The result is guaranteed to be 0 or greater.
- While the mantissa is non-zero, output `0x80 + leading byte` as above and update the mantissa to be the remainder.
- Output `0x00`.

As a description of how this produces the correct ordering, consider the result of comparison in the first differing
byte:

- Difference in the first byte can be caused by:
  - Difference in sign of the number or being zero, which yields the correct ordering because
    - Negative numbers start with `0x3c` - `0x44`
    - Zero starts with `0x80`
    - Positive numbers start with `0xbc` - `0xc4`
  - Difference in sign of the exponent modulated with the sign of the number. In a positive number negative exponents
    mean smaller values, while in a negative number it’s the opposite, thus the modulation with the number’s sign
    ensures the correct ordering.
  - Difference in modulated length of the exponent: again, since we gave the length a sign that is formed from both
    the sign of the exponent and the sign of the number, smaller numbers mean smaller exponent in the positive number
    case, and bigger exponent in the negative number case. In either case this provides the correct ordering.
- Difference in one of the bytes of the modulated exponent (whose length and sign are now equal for both compared
  numbers):
  - Smaller byte means a smaller modulated exponent. In the positive case this means a smaller exponent, thus a smaller
    number. In the negative case this means the exponent is bigger, the absolute value of the number as well, and thus
    the number is smaller.
- It is not possible for the difference to mix one number’s exponent with another’s mantissa (as such numbers would have
  different leading bytes).
- Difference in a mantissa byte present in both inputs:
  - Smaller byte means smaller signed mantissa and hence smaller number when the exponents are equal.
- One mantissa ending before another:
  - This will result in the shorter being treated as smaller (since the trailing byte is `00`).
  - Since all mantissas have at least one byte, this can’t happen in the leading mantissa byte.
  - Thus the other number’s bytes from here on are not negative, and at least one of them must be non-zero, which means
    its mantissa is bigger and thus it encodes a bigger number.

Examples:


|      value |  mexp | mantissa | mantissa in bytes | encodes as           |
| ---------: | ----: | -------- | ----------------- | -------------------- |
|        1.1 |     1 | 0.0110   | .  01 10          | C1·01·81 8A·00    |
|          1 |     1 | 0.01     | .  01             | C1·01·81·00       |
|       0.01 |     0 | 0.01     | .  01             | C0·81·00           |
|          0 |       |          |                   | 80                   |
|      -0.01 |     0 | -0.01    | . -01             | 40·81·00           |
|         -1 |    -1 | -0.01    | . -01             | 3F·FF·7F·00       |
|       -1.1 |    -1 | -0.0110  | . -02 90          | 3F·FF·7E DA·00    |
|      -98.9 |    -1 | -0.9890  | . -99 10          | 3F·FF·1D 8A·00    |
|        -99 |    -1 | -0.99    | . -99             | 3F·FF·1D·00       |
|      -99.9 |    -1 | -0.9990  | .-100 10          | 3F·FF·1C 8A·00    |
|  -8.1e2000 | -1001 | -0.0810  | . -09 90          | 3E·FC 17·77 DA·00 |
| -8.1e-2000 |   999 | -0.0810  | . -09 90          | 42·03 E7·77 DA·00 |
|  8.1e-2000 |  -999 | 0.0810   | .  08 10          | BE·FC 19·88 8A·00 |
|   8.1e2000 |  1001 | 0.0810   | .  08 10          | C2·03 E9·88 8A·00 |

(mexp stands for “modulated exponent”, i.e. exponent * sign)

The values are prefix-free, because no exponent’s encoding can be a prefix of another, and the mantissas can never have
a `00` byte at any place other than the last byte, and thus all (1)-(4) are satisfied.

## Nulls and empty encodings

Some types in Cassandra (e.g. numbers) admit null values that are represented as empty byte buffers. This is
distinct from null byte buffers, which can also appear in some cases. Particularly, null values in clustering
columns, when allowed by the type, are interpreted as empty byte buffers, encoded with the empty separator `0x3F`.
Unspecified clustering columns (at the end of a clustering specification), possible with `COMPACT STORAGE` or secondary
indexes, use the null separator `0x3E`.

## Reversed types

Reversing a type is straightforward: flip all bits of the encoded byte sequence. Since the source type encoding must
satisfy (3) and (4), the flipped bits also do for the reversed comparator. (It is also true that if the source type
satisfies (1)-(2), the reversed will satisfy these too.)

In a sequence we also must correct the empty encoding for a reversed type (since it must be greater than all values).
Instead of `0x3F` we use `0x41` as the separator byte. Null encodings are not modified, as nulls compare smaller even
in reversed types.
