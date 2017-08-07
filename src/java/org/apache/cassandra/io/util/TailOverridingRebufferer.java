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
package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

/**
 * Special rebufferer that replaces the tail of the file (from the specified cutoff point) with the given buffer.
 * Thread-unsafe, to be instantiated for each reader. Reuses itself as the buffer returned.
 */
public class TailOverridingRebufferer extends WrappingRebufferer
{
    final long cutoff;
    final ByteBuffer tail;

    public TailOverridingRebufferer(Rebufferer source, long cutoff, ByteBuffer tail)
    {
        super(source);
        this.cutoff = cutoff;
        this.tail = tail;
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        return rebuffer(position, ReaderConstraint.NONE);
    }

    @Override
    public BufferHolder rebuffer(long position, ReaderConstraint constraint)
    {
        assert bufferHolder == null;
        if (position < cutoff)
        {
            super.rebuffer(position, constraint);
            if (offset + buffer.limit() > cutoff)
                buffer.limit((int) (cutoff - offset));
        }
        else
        {
            buffer = tail.duplicate();
            offset = cutoff;
        }

        return this;
    }


    @Override
    public long fileLength()
    {
        return cutoff + tail.limit();
    }

    @Override
    public void release()
    {
        if (bufferHolder != null)
        {
            bufferHolder.release();
            bufferHolder = null;
        }
    }

    @Override
    public String paramsToString()
    {
        return String.format("+%d@%d", tail.limit(), cutoff);
    }
}