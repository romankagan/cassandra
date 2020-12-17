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
package org.apache.cassandra.utils.bytecomparable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.ValueAccessor;

/**
 * Contains various utilities for working with {@link ByteSource}s.
 */
public final class ByteSourceUtil
{
    private static final int INITIAL_BUFFER_CAPACITY = 32;

    private static final int SIGN_MASK = 0b10000000;

    private static long getSignedFixedLengthAsLong(ByteSource byteSource, int length)
    {
        if (byteSource == null)
            throw new IllegalArgumentException("Unexpected null ByteSource");
        if (length < 1 || length > 8)
            throw new IllegalArgumentException("Between 1 and 8 bytes can be read at a time");

        // If the decoded value is negative, pad the preceding buffer bytes with 1s, so that in case the value uses
        // less than 8 bytes, the buffer value as long is also a correct representation of the decoded value (as an
        // integral type using less than 8 bytes). This padding can be done by just widening the sign-corrected byte
        // value to long.
        long result = (byte) (byteSource.next() ^ SIGN_MASK);
        for (int i = 1; i < length; ++i)
        {
            int data = byteSource.next();
            if (data == ByteSource.END_OF_STREAM)
                throw new IllegalArgumentException(
                        String.format("Unexpected end of stream reached after %d bytes (expected >= %d)", i, length));
            assert data >= 0 && data <= 255;
            result = (result << 8) | data;
        }
        return result;
    }

    public static <V> V getSignedFixedLength(ValueAccessor<V> accessor, ByteSource byteSource, int length)
    {
        if (byteSource == null)
            throw new IllegalArgumentException("Unexpected null ByteSource");
        if (length < 1)
            throw new IllegalArgumentException("At least 1 byte should be read");

        // Maybe consider passing down allocation strategy (on-heap vs direct)?
        V result = accessor.allocate(length);
        accessor.putByte(result, 0, (byte) (byteSource.next() ^ SIGN_MASK));
        for (int i = 1; i < length; ++i)
        {
            int data = byteSource.next();
            if (data == ByteSource.END_OF_STREAM)
                throw new IllegalArgumentException(
                        String.format("Unexpected end of stream reached after %d bytes (expected >= %d)", i, length));
            assert data >= 0 && data <= 255;
            accessor.putByte(result, i, (byte) data);
        }
        return result;
    }

    public static <V> V getOptionalSignedFixedLength(ValueAccessor<V> accessor, ByteSource byteSource, int length)
    {
        return byteSource == null ? accessor.empty() : getSignedFixedLength(accessor, byteSource, length);
    }

    public static <V> V getSignedFixedLengthFloat(ValueAccessor<V> accessor, ByteSource byteSource, int length)
    {
        if (byteSource == null)
            throw new IllegalArgumentException("Unexpected null ByteSource");
        if (length < 1)
            throw new IllegalArgumentException("At least 1 byte should be read");

        // Maybe consider passing down allocation strategy (on-heap vs direct)?
        V result = accessor.allocate(length);
        int first = byteSource.next() & 0xFF;
        int xor = 0;
        if (first < 0x80) // the encoding puts negative numbers below positives
        {
            xor = 0xFF; // invert all bits
            first ^= xor;
        }
        else
            first ^= SIGN_MASK;
        accessor.putByte(result, 0, (byte) first);

        for (int i = 1; i < length; ++i)
        {
            int data = byteSource.next();
            if (data == ByteSource.END_OF_STREAM)
                throw new IllegalArgumentException(
                String.format("Unexpected end of stream reached after %d bytes (expected >= %d)", i, length));
            assert data >= 0 && data <= 255;
            data ^= xor;
            accessor.putByte(result, i, (byte) data);
        }
        return result;
    }

    public static <V> V getOptionalSignedFixedLengthFloat(ValueAccessor<V> accessor, ByteSource byteSource, int length)
    {
        return byteSource == null ? accessor.empty() : getSignedFixedLengthFloat(accessor, byteSource, length);
    }

    public static <V> V getFixedLength(ValueAccessor<V> accessor, ByteSource byteSource, int length)
    {
        if (byteSource == null)
            throw new IllegalArgumentException("Unexpected null ByteSource");
        if (length < 1)
            throw new IllegalArgumentException("At least 1 byte should be read");

        // Maybe consider passing down allocation strategy (on-heap vs direct)?
        V result = accessor.allocate(length);
        for (int i = 0; i < length; ++i)
        {
            int data = byteSource.next();
            if (data == ByteSource.END_OF_STREAM)
                throw new IllegalArgumentException(
                        String.format("Unexpected end of stream reached after %d bytes (expected >= %d)", i, length));
            assert data >= 0 && data <= 255;
            accessor.putByte(result, i, (byte) data);
        }
        return result;
    }

    public static <V> V getOptionalFixedLength(ValueAccessor<V> accessor, ByteSource byteSource, int length)
    {
        return byteSource == null ? accessor.empty() : getFixedLength(accessor, byteSource, length);
    }

    /**
     * Gets the next {@code int} from the current position of the given {@link ByteSource}. The source position is
     * modified accordingly (moved 4 bytes forward).
     * <p>
     * The source is not strictly required to represent just the encoding of an {@code int} value, so theoretically
     * this API could be used for reading data in 4-byte strides. Nevertheless its usage is fairly limited because:
     * <ol>
     *     <li>...it presupposes signed fixed-length encoding for the encoding of the original value</li>
     *     <li>...it decodes the data returned on each stride as an {@code int} (i.e. it inverts its leading bit)</li>
     *     <li>...it doesn't provide any meaningful guarantees (with regard to throwing) in case there are not enough
     *     bytes to read, in case a special escape value was not interpreted as such, etc.</li>
     * </ol>
     * </p>
     *
     * @param byteSource A non-null byte source, containing at least 4 bytes.
     */
    public static int getInt(ByteSource byteSource)
    {
        return (int) getSignedFixedLengthAsLong(byteSource, 4);
    }

    /**
     * Gets the next {@code long} from the current position of the given {@link ByteSource}. The source position is
     * modified accordingly (moved 8 bytes forward).
     * <p>
     * The source is not strictly required to represent just the encoding of a {@code long} value, so theoretically
     * this API could be used for reading data in 8-byte strides. Nevertheless its usage is fairly limited because:
     * <ol>
     *     <li>...it presupposes signed fixed-length encoding for the encoding of the original value</li>
     *     <li>...it decodes the data returned on each stride as a {@code long} (i.e. it inverts its leading bit)</li>
     *     <li>...it doesn't provide any meaningful guarantees (with regard to throwing) in case there are not enough
     *     bytes to read, in case a special escape value was not interpreted as such, etc.</li>
     * </ol>
     * </p>
     *
     * @param byteSource A non-null byte source, containing at least 8 bytes.
     */
    public static long getLong(ByteSource byteSource)
    {
        return getSignedFixedLengthAsLong(byteSource, 8);
    }

    /**
     * Converts the given {@link ByteSource} to a {@code byte}.
     *
     * @param byteSource A non-null byte source, containing at least 1 byte.
     */
    public static byte getByte(ByteSource byteSource)
    {
        if (byteSource == null)
            throw new IllegalArgumentException("Unexpected null ByteSource");
        int theByte = byteSource.next();
        if (theByte == ByteSource.END_OF_STREAM)
            throw new IllegalArgumentException("Unexpected ByteSource with length 0 instead of 1");

        return (byte) ((theByte & 0xFF) ^ (1 << 7));
    }

    /**
     * Converts the given {@link ByteSource} to a {@code short}. All terms and conditions valid for
     * {@link #getInt(ByteSource)} and {@link #getLong(ByteSource)} translate to this as well.
     *
     * @param byteSource A non-null byte source, containing at least 2 bytes.
     *
     * @see #getInt(ByteSource)
     * @see #getLong(ByteSource)
     */
    public static short getShort(ByteSource byteSource)
    {
        return (short) getSignedFixedLengthAsLong(byteSource, 2);
    }

    /**
     * Extract the bytes of a UUID from the given {@link ByteSource}. If the given source doesn't represent a variant 2
     * (Leach-Salz) {@link UUID}, anything from a wide variety of throwables may be thrown ({@link AssertionError},
     * {@link IndexOutOfBoundsException}, {@link IllegalStateException}, etc.).
     *
     * @param byteSource The source we're interested in.
     * @param uuidType The abstract UUID type with which the UUID value have been encoded.
     * @return the bytes of the {@link UUID} corresponding to the given source.
     *
     * @see UUID
     */
    public static <V> V getUuidBytes(ValueAccessor<V> accessor, ByteSource byteSource, AbstractType<UUID> uuidType)
    {
        // Optional-style encoding of empty values as null sources
        if (byteSource == null)
            return accessor.empty();

        long hiBits = getLong(byteSource);
        long loBits = getLong(byteSource);

        if (uuidType instanceof LexicalUUIDType)
        {
            // Lexical UUIDs are stored as just two longs (with their sign bits flipped). The simple decoding of these
            // longs flips their sign bit back, so they can directly be used for constructing the original UUID.
            return makeUuidBytes(accessor, hiBits, loBits);
        }

        assert uuidType instanceof UUIDType || uuidType instanceof TimeUUIDType;

        // The non-lexical UUID bits are stored as an unsigned fixed-length 128-bit integer, which we can easily
        // consume as two consecutive longs, but we need to account for the sign correction that happens (in this case
        // unnecessarily) when we decode signed fixed-length longs.
        hiBits ^= 1L << 63;
        loBits ^= 1L << 63;

        long version = hiBits >>> 60 & 0xF;
        if (version == 1)
        {
            // If the version bits are set to 1, this is a time-based UUID, and its high bits are significantly more
            // shuffled than in other UUIDs - if [X] represents a 16-bit tuple, [1][2][3][4] should become [3][4][2][1].
            // See the UUID Javadoc (and more specifically the high bits layout of a Leach-Salz UUID) to understand the
            // reasoning behind this bit twiddling in the first place (in the context of comparisons).
            hiBits = hiBits << 32
                     | hiBits >>> 16 & 0xFFFF0000L
                     | hiBits >>> 48;
            // In addition, TimeUUIDType also touches the low bits of the UUID (see CASSANDRA-8730 and DB-1758).
            if (uuidType instanceof TimeUUIDType)
                loBits ^= 0x8080808080808080L;
        }
        else
        {
            // Non-time UUIDs can be handled here solely by UUIDType (LexicalUUIDType would have been handled earlier).
            assert uuidType instanceof UUIDType;
            // Otherwise the only thing that's needed is to put the version bits back where they were originally.
            hiBits = hiBits << 4 & 0xFFFFFFFFFFFF0000L
                     | version << 12
                     | hiBits & 0x0000000000000FFFL;
        }

        return makeUuidBytes(accessor, hiBits, loBits);
    }

    private static <V> V makeUuidBytes(ValueAccessor<V> accessor, long high, long low)
    {
        V buffer = accessor.allocate(16);
        accessor.putLong(buffer, 0, high);
        accessor.putLong(buffer, 8, low);
        return buffer;
    }

    /**
     * Reads a single variable-length byte sequence (blob, string, ...) encoded according to the scheme from
     * https://docs.google.com/document/d/1X9S69gh2-qkb5DyWOFtu-DAr4tBKtzmGJP-Glr8uKL8, decoding it back to its
     * original, unescaped form.
     *
     * @param byteSource The source of the variable-length bytes sequence.
     * @return A byte array containing the original, unescaped bytes of the given source. Unescaped here means
     * not including any of the escape sequences of the encoding scheme used for variable-length byte sequences.
     */
    public static byte[] getUnescapedBytes(ByteSource.Peekable byteSource)
    {
        return byteSource == null ? null : readBytes(unescape(byteSource));
    }

    /**
     * As above, but converts the result to a ByteSource. Used directly by SAI to convert from the prefix-free
     * Memtable encoding to on-disk tries.
     */
    public static ByteSource unescape(ByteSource.Peekable byteSource)
    {
        return new ByteSource() {
            boolean escaped = false;

            public int next()
            {
                if (!escaped)
                {
                    int data = byteSource.next(); // we consume this byte no matter what it is
                    if (data > ByteSource.ESCAPE)
                        return data;        // most used path leads here

                    assert data != ByteSource.END_OF_STREAM : "Invalid escaped byte sequence";
                    escaped = true;
                }

                int next = byteSource.peek();
                switch (next)
                {
                    case END_OF_STREAM:
                        // The end of a byte-comparable outside of a multi-component sequence. No matter what we have
                        // seen or peeked before, we should stop now.
                        byteSource.next();
                        return END_OF_STREAM;
                    case ESCAPED_0_DONE:
                        // The end of 1 or more consecutive 0x00 value bytes.
                        escaped = false;
                        byteSource.next();
                        return ESCAPE;
                    case ESCAPED_0_CONT:
                        // Escaped sequence continues
                        byteSource.next();
                        return ESCAPE;
                    default:
                        // An escaping ESCAPED_0_CONT won't be followed by either another ESCAPED_0_CONT, an
                        // ESCAPED_0_DONE, or an END_OF_STREAM only when the byte-comparable is part of a multi-component
                        // sequence, we have reached the end of the encoded byte-comparable, and the last value byte of
                        // the byte-comparable is not a 0x00 byte. In this case, the 0x00 byte that lead us here is the
                        // byte actually signifying the end of the byte-comparable, and the byte that we have last peeked
                        // should be the final terminator/separator byte of the byte-comparable component.
                        //
                        // P.S. In the older DSE6 encoding which we do not decode, this can also happen if the last value
                        // byte of the byte-comparable is a 0x00 byte. In this case, that byte should have been escaped
                        // by an ESCAPED_0_CONT that we should have just consumed. The byte that we have last peeked then
                        // should be another 0x00 byte - the one actually signifying the end of the byte-comparable, and
                        // that last 0x00 byte then should be followed by the final terminator/separator byte.
                        assert next >= ByteSource.MIN_SEPARATOR && next <= ByteSource.MAX_SEPARATOR : next;
                        // Unlike above, we don't consume this byte (the sequence decoding needs it).
                        return END_OF_STREAM;
                }
            }
        };
    }

    /**
     * Reads the bytes of the given source into a byte array. Doesn't do any transformation on the bytes, just reads
     * them until it reads an {@link ByteSource#END_OF_STREAM} byte, after which it returns an array of all the read
     * bytes, <strong>excluding the {@link ByteSource#END_OF_STREAM}</strong>.
     * <p>
     * This method sizes a tentative internal buffer array at {@code initialBufferCapacity}.  However, if
     * {@code byteSource} exceeds this size, the buffer array is recreated with doubled capacity as many times as
     * necessary.  If, after {@code byteSource} is fully exhausted, the number of bytes read from it does not exactly
     * match the current size of the tentative buffer array, then it is copied into another array sized to fit the
     * number of bytes read; otherwise, it is returned without that final copy step.
     *
     * @param byteSource The source which bytes we're interested in.
     * @param initialBufferCapacity The initial size of the internal buffer.
     * @return A byte array containing exactly all the read bytes. In case of a {@code null} source, the returned byte
     * array will be empty.
     */
    public static byte[] readBytes(ByteSource byteSource, final int initialBufferCapacity)
    {
        if (byteSource == null)
            return new byte[0];

        int readBytes = 0;
        byte[] buf = new byte[initialBufferCapacity];
        int data;
        while ((data = byteSource.next()) != ByteSource.END_OF_STREAM)
        {
            buf = ensureCapacity(buf, readBytes);
            buf[readBytes++] = (byte) (data & 0xFF);
        }

        if (readBytes != buf.length)
        {
            buf = Arrays.copyOf(buf, readBytes);
        }
        return buf;
    }

    /**
     * Reads the bytes of the given source into a byte array. Doesn't do any transformation on the bytes, just reads
     * them until it reads an {@link ByteSource#END_OF_STREAM} byte, after which it returns an array of all the read
     * bytes, <strong>excluding the {@link ByteSource#END_OF_STREAM}</strong>.
     * <p>
     * This is equivalent to {@link #readBytes(ByteSource, int)} where the second actual parameter is
     * {@linkplain #INITIAL_BUFFER_CAPACITY} ({@value INITIAL_BUFFER_CAPACITY}).
     *
     * @param byteSource The source which bytes we're interested in.
     * @return A byte array containing exactly all the read bytes. In case of a {@code null} source, the returned byte
     * array will be empty.
     */
    public static byte[] readBytes(ByteSource byteSource)
    {
        return readBytes(byteSource, INITIAL_BUFFER_CAPACITY);
    }

    /**
     * Ensures the given buffer has capacity for taking data with the given length - if it doesn't, it returns a copy
     * of the buffer, but with double the capacity.
     */
    private static byte[] ensureCapacity(byte[] buf, int dataLengthInBytes)
    {
        if (dataLengthInBytes == buf.length)
            // We won't gain much with guarding against overflow. We'll overflow when dataLengthInBytes >= 1 << 30,
            // and if we do guard, we'll be able to extend the capacity to Integer.MAX_VALUE (which is 1 << 31 - 1).
            // Controlling the exception that will be thrown shouldn't matter that much, and  in practice, we almost
            // surely won't be reading gigabytes of ByteSource data at once.
            return Arrays.copyOf(buf, dataLengthInBytes * 2);
        else
            return buf;
    }

    /**
     * Converts the given {@link ByteSource} to a UTF-8 {@link String}.
     *
     * @param byteSource The source we're interested in.
     * @return A UTF-8 string corresponding to the given source.
     */
    public static String getString(ByteSource.Peekable byteSource)
    {
        if (byteSource == null)
            return null;

        byte[] data = getUnescapedBytes(byteSource);

        return new String(data, StandardCharsets.UTF_8);
    }

    /*
     * Multi-component sequence utilities.
     */

    /**
     * A utility for consuming components from a peekable multi-component sequence.
     * It uses the component separators, so the given sequence needs to have its last component fully consumed, in
     * order for the next consumable byte to be a separator. Identifying the end of the component that will then be
     * consumed is the responsibility of the consumer (the user of this method).
     * @param source A peekable multi-component sequence, which next byte is a component separator.
     * @return the given multi-component sequence if its next component is not null, or {@code null} if it is.
     */
    public static ByteSource.Peekable nextComponentSource(ByteSource.Peekable source)
    {
        int separator = source.next();
        return nextComponentNull(separator)
               ? null
               : source;
    }

    /**
     * A utility for consuming components from a peekable multi-component sequence, very similar to
     * {@link #nextComponentSource(ByteSource.Peekable)} - the difference being that here the separator can be passed
     * in case it had to be consumed beforehand.
     */
    public static ByteSource.Peekable nextComponentSource(ByteSource.Peekable source, int separator)
    {
        return nextComponentNull(separator)
               ? null
               : source;
    }

    public static boolean nextComponentNull(int separator)
    {
        return separator == ByteSource.NEXT_COMPONENT_NULL || separator == ByteSource.NEXT_COMPONENT_NULL_REVERSED;
    }
}
