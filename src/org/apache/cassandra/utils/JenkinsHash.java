package org.apache.cassandra.utils;

class JenkinsHash
{

	// max value to limit it to 4 bytes
	private static final long MAX_VALUE = 0xFFFFFFFFL;

	// internal variables used in the various calculations
	private long a_;
	private long b_;
	private long c_;

	/**
	 * Convert a byte into a long value without making it negative.
	 */
	private long byteToLong(byte b)
	{
		long val = b & 0x7F;
		if ((b & 0x80) != 0)
		{
			val += 128;
		}
		return val;
	}

	/**
	 * Do addition and turn into 4 bytes.
	 */
	private long add(long val, long add)
	{
		return (val + add) & MAX_VALUE;
	}

	/**
	 * Do subtraction and turn into 4 bytes.
	 */
	private long subtract(long val, long subtract)
	{
		return (val - subtract) & MAX_VALUE;
	}

	/**
	 * Left shift val by shift bits and turn in 4 bytes.
	 */
	private long xor(long val, long xor)
	{
		return (val ^ xor) & MAX_VALUE;
	}

	/**
	 * Left shift val by shift bits. Cut down to 4 bytes.
	 */
	private long leftShift(long val, int shift)
	{
		return (val << shift) & MAX_VALUE;
	}

	/**
	 * Convert 4 bytes from the buffer at offset into a long value.
	 */
	private long fourByteToLong(byte[] bytes, int offset)
	{
		return (byteToLong(bytes[offset + 0])
				+ (byteToLong(bytes[offset + 1]) << 8)
				+ (byteToLong(bytes[offset + 2]) << 16) + (byteToLong(bytes[offset + 3]) << 24));
	}

	/**
	 * Mix up the values in the hash function.
	 */
	private void hashMix()
	{
		a_ = subtract(a_, b_);
		a_ = subtract(a_, c_);
		a_ = xor(a_, c_ >> 13);
		b_ = subtract(b_, c_);
		b_ = subtract(b_, a_);
		b_ = xor(b_, leftShift(a_, 8));
		c_ = subtract(c_, a_);
		c_ = subtract(c_, b_);
		c_ = xor(c_, (b_ >> 13));
		a_ = subtract(a_, b_);
		a_ = subtract(a_, c_);
		a_ = xor(a_, (c_ >> 12));
		b_ = subtract(b_, c_);
		b_ = subtract(b_, a_);
		b_ = xor(b_, leftShift(a_, 16));
		c_ = subtract(c_, a_);
		c_ = subtract(c_, b_);
		c_ = xor(c_, (b_ >> 5));
		a_ = subtract(a_, b_);
		a_ = subtract(a_, c_);
		a_ = xor(a_, (c_ >> 3));
		b_ = subtract(b_, c_);
		b_ = subtract(b_, a_);
		b_ = xor(b_, leftShift(a_, 10));
		c_ = subtract(c_, a_);
		c_ = subtract(c_, b_);
		c_ = xor(c_, (b_ >> 15));
	}

	/**
	 * Hash a variable-length key into a 32-bit value. Every bit of the key
	 * affects every bit of the return value. Every 1-bit and 2-bit delta
	 * achieves avalanche. The best hash table sizes are powers of 2.
	 * 
	 * @param buffer
	 *            Byte array that we are hashing on.
	 * @param initialValue
	 *            Initial value of the hash if we are continuing from a previous
	 *            run. 0 if none.
	 * @return Hash value for the buffer.
	 */
	public long hash(byte[] buffer, long initialValue)
	{
		int len, pos;

		// set up the internal state
		// the golden ratio; an arbitrary value
		a_ = 0x09e3779b9L;
		// the golden ratio; an arbitrary value
		b_ = 0x09e3779b9L;
		// the previous hash value
		c_ = initialValue;

		// handle most of the key
		pos = 0;
		for (len = buffer.length; len >= 12; len -= 12)
		{
			a_ = add(a_, fourByteToLong(buffer, pos));
			b_ = add(b_, fourByteToLong(buffer, pos + 4));
			c_ = add(c_, fourByteToLong(buffer, pos + 8));
			hashMix();
			pos += 12;
		}

		c_ += buffer.length;

		// all the case statements fall through to the next on purpose
		switch (len)
		{
			case 11:
				c_ = add(c_, leftShift(byteToLong(buffer[pos + 10]), 24));
			case 10:
				c_ = add(c_, leftShift(byteToLong(buffer[pos + 9]), 16));
			case 9:
				c_ = add(c_, leftShift(byteToLong(buffer[pos + 8]), 8));
				// the first byte of c is reserved for the length
			case 8:
				b_ = add(b_, leftShift(byteToLong(buffer[pos + 7]), 24));
			case 7:
				b_ = add(b_, leftShift(byteToLong(buffer[pos + 6]), 16));
			case 6:
				b_ = add(b_, leftShift(byteToLong(buffer[pos + 5]), 8));
			case 5:
				b_ = add(b_, byteToLong(buffer[pos + 4]));
			case 4:
				a_ = add(a_, leftShift(byteToLong(buffer[pos + 3]), 24));
			case 3:
				a_ = add(a_, leftShift(byteToLong(buffer[pos + 2]), 16));
			case 2:
				a_ = add(a_, leftShift(byteToLong(buffer[pos + 1]), 8));
			case 1:
				a_ = add(a_, byteToLong(buffer[pos + 0]));
				// case 0: nothing left to add
		}
		hashMix();

		return c_;
	}

	/**
	 * See hash(byte[] buffer, long initialValue)
	 * 
	 * @param buffer
	 *            Byte array that we are hashing on.
	 * @return Hash value for the buffer.
	 */
	public long hash(byte[] buffer)
	{
		return hash(buffer, 0);
	}
}

