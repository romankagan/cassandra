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

package org.apache.cassandra.utils;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;

import net.nicoulaj.compilecommand.annotations.Inline;

import static org.apache.cassandra.utils.Architecture.IS_UNALIGNED;
import static org.apache.cassandra.utils.FastByteOperations.UnsafeOperations.theUnsafe;

public class UnsafeByteBufferAccess
{
    public static final long DIRECT_BYTE_BUFFER_LIMIT_OFFSET;
    public static final long DIRECT_BYTE_BUFFER_POSITION_OFFSET;
    public static final long DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET;
    public static final long BYTE_BUFFER_OFFSET_OFFSET;
    public static final long BYTE_BUFFER_HB_OFFSET;
    public static final long BYTE_BUFFER_NATIVE_ORDER;
    public static final long BYTE_BUFFER_BIG_ENDIAN;

    public static final long DIRECT_BYTE_BUFFER_ADDRESS_OFFSET;
    public static final long DIRECT_BYTE_BUFFER_CAPACITY_OFFSET;

    public static final Class<?> DIRECT_BYTE_BUFFER_CLASS;
    public static final Class<?> DIRECT_BYTE_BUFFER_R_CLASS;
    public static final long BYTE_ARRAY_BASE_OFFSET;

    /**
     * Declare an empty direct byte buffer here that can be used instead of calling  ByteBuffer.allocateDirect(0).
     * This is because the JVM allocates 1 byte even if calling allocateDirect(0) and this results in the nio memory
     * counters for used and reserved memory to be off by one byte. However, we tell our users
     * to expect these values to be equal. So we create our own empty buffer with a null address;
     */
    public static final ByteBuffer EMPTY_BUFFER;

    static
    {
        try
        {
            DIRECT_BYTE_BUFFER_ADDRESS_OFFSET = theUnsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            DIRECT_BYTE_BUFFER_CAPACITY_OFFSET = theUnsafe.objectFieldOffset(Buffer.class.getDeclaredField("capacity"));
            DIRECT_BYTE_BUFFER_LIMIT_OFFSET = theUnsafe.objectFieldOffset(Buffer.class.getDeclaredField("limit"));
            DIRECT_BYTE_BUFFER_POSITION_OFFSET = theUnsafe.objectFieldOffset(Buffer.class.getDeclaredField("position"));

            DIRECT_BYTE_BUFFER_CLASS = ByteBuffer.allocateDirect(0).getClass();
            DIRECT_BYTE_BUFFER_R_CLASS = ByteBuffer.allocateDirect(0).asReadOnlyBuffer().getClass();
            DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET = theUnsafe.objectFieldOffset(DIRECT_BYTE_BUFFER_CLASS.getDeclaredField("att"));

            BYTE_BUFFER_OFFSET_OFFSET = theUnsafe.objectFieldOffset(ByteBuffer.class.getDeclaredField("offset"));
            BYTE_BUFFER_HB_OFFSET = theUnsafe.objectFieldOffset(ByteBuffer.class.getDeclaredField("hb"));
            BYTE_BUFFER_NATIVE_ORDER = theUnsafe.objectFieldOffset(ByteBuffer.class.getDeclaredField("nativeByteOrder"));
            BYTE_BUFFER_BIG_ENDIAN = theUnsafe.objectFieldOffset(ByteBuffer.class.getDeclaredField("bigEndian"));

            BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

            EMPTY_BUFFER = allocateHollowDirectByteBuffer(); // null address and capacity set to 0
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    @Inline
    public static long getAddress(ByteBuffer buffer)
    {
        return theUnsafe.getLong(buffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET);
    }

    @Inline
    public static Object getArray(ByteBuffer buffer)
    {
        return theUnsafe.getObject(buffer, BYTE_BUFFER_HB_OFFSET);
    }

    @Inline
    public static int getOffset(ByteBuffer buffer)
    {
        return theUnsafe.getInt(buffer, BYTE_BUFFER_OFFSET_OFFSET);
    }

    @Inline
    public static boolean nativeByteOrder(ByteBuffer buffer)
    {
        return theUnsafe.getBoolean(buffer, BYTE_BUFFER_NATIVE_ORDER);
    }

    @Inline
    public static boolean bigEndian(ByteBuffer buffer)
    {
        return theUnsafe.getBoolean(buffer, BYTE_BUFFER_BIG_ENDIAN);
    }

    public static Object getAttachment(ByteBuffer instance)
    {
        assert instance.getClass() == DIRECT_BYTE_BUFFER_CLASS || instance.getClass() == DIRECT_BYTE_BUFFER_R_CLASS
                : instance.getClass().getName();
        return theUnsafe.getObject(instance, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET);
    }

    public static void setAttachment(ByteBuffer instance, Object next)
    {
        assert instance.getClass() == DIRECT_BYTE_BUFFER_CLASS || instance.getClass() == DIRECT_BYTE_BUFFER_R_CLASS
                : instance.getClass().getName();
        theUnsafe.putObject(instance, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET, next);
    }

    @Inline
    static long bufferOffset(ByteBuffer buffer, Object array)
    {
        long srcOffset;
        if (array != null)
        {
            srcOffset = BYTE_ARRAY_BASE_OFFSET + getOffset(buffer);
        }
        else
        {
            srcOffset = getAddress(buffer);
        }
        return srcOffset;
    }

    /**
     * Why do this? because the JDK only bothers optimising DirectByteBuffer for the unaligned case.
     */
    @Inline
    public static short getShort(ByteBuffer bb)
    {
        Object array = getArray(bb);
        long srcOffset = bb.position() + bufferOffset(bb, array);

        if (IS_UNALIGNED)
        {
            short x = theUnsafe.getShort(array, srcOffset);
            return (nativeByteOrder(bb) ? x : Short.reverseBytes(x));
        }
        else
            return UnsafeMemoryAccess.getShortByByte(array, srcOffset, bigEndian(bb));
    }

    /**
     * Why do this? because the JDK only bothers optimising DirectByteBuffer for the unaligned case.
     */
    @Inline
    public static int getInt(ByteBuffer bb)
    {
        Object array = getArray(bb);
        long srcOffset = bb.position() + bufferOffset(bb, array);

        if (IS_UNALIGNED)
        {
            int x = theUnsafe.getInt(array, srcOffset);
            return (nativeByteOrder(bb)  ? x : Integer.reverseBytes(x));
        }
        else
            return UnsafeMemoryAccess.getIntByByte(array, srcOffset, bigEndian(bb));
    }

    /**
     * Why do this? because the JDK only bothers optimising DirectByteBuffer for the unaligned case.
     */
    @Inline
    public static long getLong(ByteBuffer bb)
    {
        Object array = getArray(bb);
        long srcOffset = bb.position() + bufferOffset(bb, array);

        if (IS_UNALIGNED)
        {
            final long l = theUnsafe.getLong(array, srcOffset);
            return (nativeByteOrder(bb) ? l : Long.reverseBytes(l));
        }
        else
            return UnsafeMemoryAccess.getLongByByte(array, srcOffset, bigEndian(bb));
    }

    /**
     * Why do this? because the JDK only bothers optimising DirectByteBuffer for the unaligned case.
     */
    @Inline
    public static double getDouble(ByteBuffer bb)
    {
        return Double.longBitsToDouble(getLong(bb));

    }

    /**
     * Why do this? because the JDK only bothers optimising DirectByteBuffer for the unaligned case.
     */
    @Inline
    public static float getFloat(ByteBuffer bb)
    {
        return Float.intBitsToFloat(getInt(bb));
    }

    /**
     * @param address the memory address to use for the new buffer
     * @param length in bytes of the new buffer
     * @return a new DirectByteBuffer setup with the address, length required, and native byte order
     */
    public static ByteBuffer allocateByteBuffer(long address, int length)
    {
        return allocateByteBuffer(address, length, ByteOrder.nativeOrder());
    }

    /**
     * @param address the memory address to use for the new buffer
     * @param length in bytes of the new buffer
     * @param order byte order of the new buffer
     * @return a new DirectByteBuffer setup with the address, length and order required
     */
    public static ByteBuffer allocateByteBuffer(long address, int length, ByteOrder order)
    {
        return allocateByteBuffer(address, length, order, null);
    }

    /**
     * @param address the memory address to use for the new buffer
     * @param length in bytes of the new buffer
     * @param order byte order of the new buffer
     * @param attachment byte buffer attachment
     * @return a new DirectByteBuffer setup with the address, length and order required
     */
    public static ByteBuffer allocateByteBuffer(long address, int length, ByteOrder order, Object attachment)
    {
        return allocateByteBuffer(address, length, length, order, attachment);
    }


    /**
     * @param address the memory address to use for the new buffer
     * @param length in bytes of the new buffer
     * @param capacity in bytes of the new buffer
     * @param order byte order of the new buffer
     * @param attachment byte buffer attachment
     * @return a new DirectByteBuffer setup with the address, length and order required
     */
    public static ByteBuffer allocateByteBuffer(long address, int length, int capacity, ByteOrder order, Object attachment)
    {
        ByteBuffer instance = allocateHollowDirectByteBuffer(order);
        initByteBufferInstance(instance, address, length, capacity);

        if (attachment != null)
            theUnsafe.putObject(instance, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET, attachment);

        return instance;
    }

    /**
     * Hollow byte buffers are used as DirectByteBuffer fly weights. They should be used with care and
     * consistently with methods prescribed below.
     *
     * @return a new DirectByteBuffer with native byte order (not ready for use as address/length are not set)
     */
    public static ByteBuffer allocateHollowDirectByteBuffer()
    {
        return allocateHollowDirectByteBuffer(ByteOrder.nativeOrder());
    }

    /**
     * @param order byte order of the new buffer
     * @return a new DirectByteBuffer with provided byte order (not ready for use as address/length are not set)
     */
    public static ByteBuffer allocateHollowDirectByteBuffer(ByteOrder order)
    {
        ByteBuffer instance;
        try
        {
            instance = (ByteBuffer) theUnsafe.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
        }
        catch (InstantiationException e)
        {
            throw new AssertionError(e);
        }
        instance.order(order);
        return instance;
    }

    /**
     * @param instance presumably a previously allocated hollow buffer
     * @param address the memory address to use for the new buffer
     * @param length in bytes of the new buffer
     */
    public static void initByteBufferInstance(ByteBuffer instance, long address, int length)
    {
        initByteBufferInstance(instance, address, length, length);
    }

    /**
     * @param instance presumably a previously allocated hollow buffer
     * @param address the memory address to use for the new buffer
     * @param length in bytes of the new buffer
     * @param capacity in bytes of the new buffer
     */
    public static void initByteBufferInstance(ByteBuffer instance, long address, int length, int capacity)
    {
        setAddress(instance, address);
        setCapacity(instance, capacity);
        setPosition(instance, 0);
        setLimit(instance, length);
    }

    /**
     * Reset a hollow buffer to explicitly prevent further use.
     *
     * @param instance presumably a previously allocated hollow buffer
     */
    public static void resetByteBufferInstance(ByteBuffer instance)
    {
        setAddress(instance, 0);
        setPosition(instance, 0);
        setCapacity(instance, 0);
        setLimit(instance, 0);
        setAttachment(instance, null);
    }

    /**
     * @param instance presumably a previously allocated hollow buffer
     * @param address the memory address to set for buffer instance
     * @param length in bytes to set for buffer instance
     * @param order byte order to set for buffer instance
     */
    public static void initByteBufferInstance(ByteBuffer instance, long address, int length, ByteOrder order)
    {
        initByteBufferInstance(instance, address, length);
        instance.order(order);
    }

    /**
     * This method has the same effect of calling {@link ByteBuffer#duplicate()} but instead of allocating, it
     * duplicates into the given hollow buffer. Note that attachment and byte order are not duplicated. Attachment
     * ref copy would potentially keep that ref alive and would require clearing it after the hollowBuffer is used.
     * Byte order not getting duplicated is consistent with {@link ByteBuffer#duplicate()}.
     *
     * @param source
     * @param hollowBuffer
     * @return hollowBuffer
     */
    public static ByteBuffer duplicateDirectByteBuffer(ByteBuffer source, ByteBuffer hollowBuffer, boolean withAttachment)
    {
        assert source.isDirect() && hollowBuffer.isDirect();
        setAddress(hollowBuffer, getAddress(source));
        setPosition(hollowBuffer, getPosition(source));
        setLimit(hollowBuffer, getLimit(source));
        setCapacity(hollowBuffer, getCapacity(source));

        if (withAttachment)
            theUnsafe.putObject(hollowBuffer, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET, theUnsafe.getObject(source, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET));

        return hollowBuffer;
    }

    /**
     * Check if a list of memory addresses represent a contiguous region of memory.
     * <p/>
     * This method returns false if:
     * <ul>
     *     <li> The addresses are not contiguous </li>
     *     <li> The addresses are not ordered </li>
     * </ul>
     *
     * @param addresses a bunch of addresses, hopefully contiguous, hopefully sorted
     * @param addressSize the size of each individual memory region
     * @param limit the required contiguous region size
     *
     * @return true if the addresses represent a contiguous region, false otherwise
     *
     * @throws IllegalArgumentException if the total size of the addresses is less than the required size.
     */
    public static boolean regionsAreContiguous(long[] addresses, int addressSize, int limit)
    {
        long start = addresses[0];
        int size = addressSize;
        for (int i = 1; i < addresses.length && size < limit; i++)
        {
            if (addresses[i] - size != start)
                return false;

            size += addressSize;
        }

        if (size < limit)
            throw new IllegalArgumentException("Total size of buffers must exceed limit");

        return true;
    }

    /**
     * Split a contiguous memory region into multiple regions of size addressSize.
     * The region was previously determined contiguous by calling {@link this#regionsAreContiguous(long[], int, int)}.
     * This method reconstitutes the original regions.
     *
     * @param address the address of the contiguous region
     * @param addressSize the size of each individual memory region
     * @param limit the total contiguous region size, must be a multiple of addressSize
     *
     * @return an array containing the addresses of the individual regions
     */
    public static long[] splitContiguousRegion(long address, int addressSize, int limit)
    {
        if (address <= 0)
            throw new IllegalStateException("Address should be valid: " + address);

        if ((limit / addressSize) * addressSize != limit)
            throw new IllegalStateException("Limit should be a multiple of address size: " + limit + ", " + addressSize);

        long[] ret = new long[limit / addressSize];
        for (int i = 0; i < ret.length; i++)
            ret[i] = address + i * addressSize;

        return ret;
    }

    private static int getCapacity(ByteBuffer source)
    {
        return theUnsafe.getInt(source, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET);
    }

    private static int getLimit(ByteBuffer source)
    {
        return theUnsafe.getInt(source, DIRECT_BYTE_BUFFER_LIMIT_OFFSET);
    }

    private static void setPosition(ByteBuffer hollowBuffer, int position)
    {
        theUnsafe.putInt(hollowBuffer, DIRECT_BYTE_BUFFER_POSITION_OFFSET, position);
    }

    private static int getPosition(ByteBuffer source)
    {
        return theUnsafe.getInt(source, DIRECT_BYTE_BUFFER_POSITION_OFFSET);
    }

    private static void setLimit(ByteBuffer instance, int length)
    {
        theUnsafe.putInt(instance, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, length);
    }

    private static void setCapacity(ByteBuffer instance, int length)
    {
        theUnsafe.putInt(instance, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, length);
    }

    private static void setAddress(ByteBuffer instance, long address)
    {
        if (instance.isReadOnly())
            throw new ReadOnlyBufferException();

        theUnsafe.putLong(instance, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, address);
    }
}
