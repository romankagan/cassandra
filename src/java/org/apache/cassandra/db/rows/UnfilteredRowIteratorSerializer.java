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
package org.apache.cassandra.db.rows;

import java.io.IOException;
import java.io.IOError;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.CsFlow;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * Serialize/Deserialize an unfiltered row iterator.
 *
 * The serialization is composed of a header, follows by the rows and range tombstones of the iterator serialized
 * until we read the end of the partition (see UnfilteredSerializer for details). The header itself
 * is:
 * {@code
 *     <cfid><key><flags><s_header>[<partition_deletion>][<static_row>][<row_estimate>]
 * where:
 *     <cfid> is the table cfid.
 *     <key> is the partition key.
 *     <flags> contains bit flags. Each flag is set if it's corresponding bit is set. From rightmost
 *         bit to leftmost one, the flags are:
 *         - is empty: whether the iterator is empty. If so, nothing follows the <flags>
 *         - is reversed: whether the iterator is in reversed clustering order
 *         - has partition deletion: whether or not there is a <partition_deletion> following
 *         - has static row: whether or not there is a <static_row> following
 *         - has row estimate: whether or not there is a <row_estimate> following
 *     <s_header> is the {@code SerializationHeader}. It contains in particular the columns contains in the serialized
 *         iterator as well as other information necessary to decoding the serialized rows
 *         (see {@code SerializationHeader.Serializer for details}).
 *     <partition_deletion> is the deletion time for the partition (delta-encoded)
 *     <static_row> is the static row for this partition as serialized by UnfilteredSerializer.
 *     <row_estimate> is the (potentially estimated) number of rows serialized. This is only used for
 *         the purpose of sizing on the receiving end and should not be relied upon too strongly.
 * }
 *
 * Please note that the format described above is the on-wire format. On-disk, the format is basically the
 * same, but the header is written once per sstable, not once per-partition. Further, the actual row and
 * range tombstones are not written using this class, but rather by
 * {@link org.apache.cassandra.io.sstable.format.trieindex.PartitionWriter}.
 */
public class UnfilteredRowIteratorSerializer extends VersionDependent<EncodingVersion>
{
    protected static final Logger logger = LoggerFactory.getLogger(UnfilteredRowIteratorSerializer.class);

    private static final int IS_EMPTY               = 0x01;
    private static final int IS_REVERSED            = 0x02;
    private static final int HAS_PARTITION_DELETION = 0x04;
    private static final int HAS_STATIC_ROW         = 0x08;
    private static final int HAS_ROW_ESTIMATE       = 0x10;

    public static final Versioned<EncodingVersion, UnfilteredRowIteratorSerializer> serializers = EncodingVersion.versioned(UnfilteredRowIteratorSerializer::new);

    private final UnfilteredSerializer unfilteredSerializer;

    private UnfilteredRowIteratorSerializer(EncodingVersion version)
    {
        super(version);
        this.unfilteredSerializer = UnfilteredSerializer.serializers.get(version);
    }

    // Should only be used for the on-wire format.
    public CsFlow<Void> serialize(FlowableUnfilteredPartition iterator, ColumnFilter selection, DataOutputPlus out) throws IOException
    {
        return serialize(iterator, selection, out, -1);
    }

    public CsFlow<Void> serialize(FlowableUnfilteredPartition partition, ColumnFilter selection, DataOutputPlus out, int rowEstimate) throws IOException
    {
        SerializationHeader header = new SerializationHeader(false,
                                                             partition.header.metadata,
                                                             partition.header.columns,
                                                             partition.header.stats);

        // Note: Emptiness check is not easy with flows. Since we skip over empty stuff anyway, we prefer not to invest
        // the effort to check and we do not perform the serialization optimization for empty partitions.

        serializeBeginningOfPartition(partition, header, selection, out, rowEstimate, false);

        return partition.content.process(unfiltered -> serialize(unfiltered, header, out))
                                .map(VOID ->
                                     {
                                         serializeEndOfPartition(out);
                                         return VOID;
                                     });
    }

    // Should only be used for the on-wire format.

    public void serialize(UnfilteredRowIterator iterator, ColumnFilter selection, DataOutputPlus out) throws IOException
    {
        serialize(iterator, selection, out, -1);
    }

    public void serialize(UnfilteredRowIterator iterator, ColumnFilter selection, DataOutputPlus out, int rowEstimate) throws IOException
    {

        SerializationHeader header = new SerializationHeader(false,
                                                             iterator.metadata(),
                                                             iterator.columns(),
                                                             iterator.stats());

        boolean isEmpty = iterator.isEmpty();
        serializeBeginningOfPartition(iterator, header, selection, out, rowEstimate, isEmpty);

        if (isEmpty)
            return;

        while (iterator.hasNext())
            serialize(iterator.next(), header, out);

        serializeEndOfPartition(out);
    }

    // Should only be used for the on-wire format.
    public void serializeBeginningOfPartition(PartitionTrait partition,
                                              SerializationHeader header,
                                              ColumnFilter selection,
                                              DataOutputPlus out,
                                              int rowEstimate,
                                              boolean isEmpty) throws IOException
    {
        assert !header.isForSSTable();

        ByteBufferUtil.writeWithVIntLength(partition.partitionKey().getKey(), out);

        int flags = 0;
        if (partition.isReverseOrder())
            flags |= IS_REVERSED;

        if (isEmpty)
        {
            out.writeByte((byte)(flags | IS_EMPTY));
            return;
        }

        DeletionTime partitionDeletion = partition.partitionLevelDeletion();
        if (!partitionDeletion.isLive())
            flags |= HAS_PARTITION_DELETION;
        Row staticRow = partition.staticRow();
        boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;
        if (hasStatic)
            flags |= HAS_STATIC_ROW;

        if (rowEstimate >= 0)
            flags |= HAS_ROW_ESTIMATE;

        out.writeByte((byte)flags);

        SerializationHeader.serializer.serializeForMessaging(header, selection, out, hasStatic);

        if (!partitionDeletion.isLive())
            header.writeDeletionTime(partitionDeletion, out);

        if (hasStatic)
            unfilteredSerializer.serialize(staticRow, header, out);

        if (rowEstimate >= 0)
            out.writeUnsignedVInt(rowEstimate);
    }

    public void serialize(Unfiltered unfiltered, SerializationHeader header, DataOutputPlus out) throws IOException
    {
        unfilteredSerializer.serialize(unfiltered, header, out);
    }

    public void serializeEndOfPartition(DataOutputPlus out) throws IOException
    {
        unfilteredSerializer.writeEndOfPartition(out);
    }

    // Please note that this consume the iterator, and as such should not be called unless we have a simple way to
    // recreate an iterator for both serialize and serializedSize, which is mostly only PartitionUpdate/ArrayBackedCachedPartition.
    public long serializedSize(UnfilteredRowIterator iterator, ColumnFilter selection, int rowEstimate)
    {
        SerializationHeader header = new SerializationHeader(false,
                                                             iterator.metadata(),
                                                             iterator.columns(),
                                                             iterator.stats());

        assert rowEstimate >= 0;

        long size = ByteBufferUtil.serializedSizeWithVIntLength(iterator.partitionKey().getKey())
                    + 1; // flags

        if (iterator.isEmpty())
            return size;

        DeletionTime partitionDeletion = iterator.partitionLevelDeletion();
        Row staticRow = iterator.staticRow();
        boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;

        size += SerializationHeader.serializer.serializedSizeForMessaging(header, selection, hasStatic);

        if (!partitionDeletion.isLive())
            size += header.deletionTimeSerializedSize(partitionDeletion);

        if (hasStatic)
            size += unfilteredSerializer.serializedSize(staticRow, header);

        if (rowEstimate >= 0)
            size += TypeSizes.sizeofUnsignedVInt(rowEstimate);

        while (iterator.hasNext())
            size += unfilteredSerializer.serializedSize(iterator.next(), header);
        size += unfilteredSerializer.serializedSizeEndOfPartition();

        return size;
    }

    public Header deserializeHeader(TableMetadata metadata, ColumnFilter selection, DataInputPlus in, SerializationHelper.Flag flag) throws IOException
    {
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.readWithVIntLength(in));
        int flags = in.readUnsignedByte();
        boolean isReversed = (flags & IS_REVERSED) != 0;
        if ((flags & IS_EMPTY) != 0)
        {
            SerializationHeader sh = new SerializationHeader(false, metadata, RegularAndStaticColumns.NONE, EncodingStats.NO_STATS);
            return new Header(sh, key, isReversed, true, null, null, 0);
        }

        boolean hasPartitionDeletion = (flags & HAS_PARTITION_DELETION) != 0;
        boolean hasStatic = (flags & HAS_STATIC_ROW) != 0;
        boolean hasRowEstimate = (flags & HAS_ROW_ESTIMATE) != 0;

        SerializationHeader header = SerializationHeader.serializer.deserializeForMessaging(in, metadata, selection, hasStatic);

        DeletionTime partitionDeletion = hasPartitionDeletion ? header.readDeletionTime(in) : DeletionTime.LIVE;

        Row staticRow = Rows.EMPTY_STATIC_ROW;
        if (hasStatic)
            staticRow = unfilteredSerializer.deserializeStaticRow(in, header, new SerializationHelper(metadata, version, flag));

        int rowEstimate = hasRowEstimate ? (int)in.readUnsignedVInt() : -1;
        return new Header(header, key, isReversed, false, partitionDeletion, staticRow, rowEstimate);
    }

    public UnfilteredRowIterator deserialize(DataInputPlus in, TableMetadata metadata, SerializationHelper.Flag flag, Header header) throws IOException
    {
        if (header.isEmpty)
            return EmptyIterators.unfilteredRow(metadata, header.key, header.isReversed);

        final SerializationHelper helper = new SerializationHelper(metadata, version, flag);
        final SerializationHeader sHeader = header.sHeader;
        return new AbstractUnfilteredRowIterator(metadata, header.key, header.partitionDeletion, sHeader.columns(), header.staticRow, header.isReversed, sHeader.stats())
        {
            private final Row.Builder builder = BTreeRow.sortedBuilder();

            protected Unfiltered computeNext()
            {
                try
                {
                    Unfiltered unfiltered = unfilteredSerializer.deserialize(in, sHeader, helper, builder);
                    return unfiltered == null ? endOfData() : unfiltered;
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        };
    }

    public UnfilteredRowIterator deserialize(DataInputPlus in, TableMetadata metadata, ColumnFilter selection, SerializationHelper.Flag flag) throws IOException
    {
        return deserialize(in, metadata, flag, deserializeHeader(metadata, selection, in, flag));
    }

    public static class Header
    {
        public final SerializationHeader sHeader;
        public final DecoratedKey key;
        public final boolean isReversed;
        public final boolean isEmpty;
        public final DeletionTime partitionDeletion;
        public final Row staticRow;
        public final int rowEstimate; // -1 if no estimate

        private Header(SerializationHeader sHeader,
                       DecoratedKey key,
                       boolean isReversed,
                       boolean isEmpty,
                       DeletionTime partitionDeletion,
                       Row staticRow,
                       int rowEstimate)
        {
            this.sHeader = sHeader;
            this.key = key;
            this.isReversed = isReversed;
            this.isEmpty = isEmpty;
            this.partitionDeletion = partitionDeletion;
            this.staticRow = staticRow;
            this.rowEstimate = rowEstimate;
        }

        @Override
        public String toString()
        {
            return String.format("{header=%s, key=%s, isReversed=%b, isEmpty=%b, del=%s, staticRow=%s, rowEstimate=%d}",
                                 sHeader, key, isReversed, isEmpty, partitionDeletion, staticRow, rowEstimate);
        }
    }
}
