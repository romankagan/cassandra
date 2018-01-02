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
package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.PrintStream;

import org.apache.cassandra.io.sstable.format.trieindex.RowIndexReader.IndexInfo;
import org.apache.cassandra.io.tries.ReverseValueIterator;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteSource;

/**
 * Reverse iterator over the row index. Needed to get previous index blocks for reverse iteration.
 */
class RowIndexReverseIterator extends ReverseValueIterator<RowIndexReverseIterator>
{
    private long currentNode = -1;

    public RowIndexReverseIterator(FileHandle file, long root, ByteSource start, ByteSource end, Rebufferer.ReaderConstraint rc)
    {
        super(file.rebuffererFactory().instantiateRebufferer(), root, start, end, true, rc);
    }

    public RowIndexReverseIterator(FileHandle file, TrieIndexEntry entry, ByteSource end, Rebufferer.ReaderConstraint rc)
    {
        this(file, entry.indexTrieRoot, ByteSource.empty(), end, rc);
    }

    /**
     * This method must be async-read-safe.
     */
    public IndexInfo nextIndexInfo()
    {
        // The IndexInfo read below may trigger a NotInCacheException. To be able to resume from that
        // without missing positions, we save and reuse the unreturned position.
        if (currentNode == -1)
        {
            currentNode = nextPayloadedNode();
            if (currentNode == -1)
                return null;
        }

        go(currentNode);
        IndexInfo info = RowIndexReader.readPayload(buf, payloadPosition(), payloadFlags());

        currentNode = -1;
        return info;
    }

    public void dumpTrie(PrintStream out)
    {
        dumpTrie(out, (buf, ppos, bits) -> {
            IndexInfo ii = RowIndexReader.readPayload(buf, ppos, bits);
            return String.format("pos %x %s", ii.offset, ii.openDeletion == null ? "" : ii.openDeletion);
        });
    }
}
