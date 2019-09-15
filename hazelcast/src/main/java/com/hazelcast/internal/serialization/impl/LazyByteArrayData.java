/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.HashUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

import static com.hazelcast.util.JVMUtil.REFERENCE_COST_IN_BYTES;

/**
 * A {@link Data} implementation where the content lives on the heap.
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public class LazyByteArrayData
        implements Data {
    // type and partition_hash are always written with BIG_ENDIAN byte-order
    public static final int PARTITION_HASH_OFFSET = 0;
    public static final int TYPE_OFFSET = 4;
    public static final int DATA_OFFSET = 8;

    public static final int HEAP_DATA_OVERHEAD = DATA_OFFSET;

    // array (12: array header, 4: length)
    private static final int ARRAY_HEADER_SIZE_IN_BYTES = 16;

    private final byte[] header;
    private final byte[] payload;

    public LazyByteArrayData(byte[] header, byte[] payload) {
        if (header != null && header.length > 0 && header.length < HEAP_DATA_OVERHEAD) {
            throw new IllegalArgumentException(
                    "Data should be either empty or should contain more than " + LazyByteArrayData.HEAP_DATA_OVERHEAD + " bytes! -> "
                            + Arrays.toString(header));
        }
        this.header = header;
        this.payload = payload;
    }

    @Override
    public int dataSize() {
        return Math.max(payload.length, 0);
    }

    @Override
    public int totalSize() {
        int headerSize = header != null ? header.length : 0;
        int payloadSize = payload != null ? payload.length : 0;
        return headerSize + payloadSize;
    }

    @Override
    public void copyTo(byte[] dest, int destPos) {
        if (totalSize() > 0) {
            System.arraycopy(header, 0, dest, destPos, header.length);
            System.arraycopy(payload, 0, dest, destPos + header.length, payload.length);
        }
    }

    @Override
    public int getPartitionHash() {
        if (hasPartitionHash()) {
            return Bits.readIntB(header, PARTITION_HASH_OFFSET);
        }
        return hashCode();
    }

    @Override
    public boolean hasPartitionHash() {
        return header != null && header.length >= HEAP_DATA_OVERHEAD && Bits.readIntB(header, PARTITION_HASH_OFFSET) != 0;
    }

    @Override
    public byte[] toByteArray() {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public int getType() {
        if (totalSize() == 0) {
            return SerializationConstants.CONSTANT_TYPE_NULL;
        }
        return Bits.readIntB(header, TYPE_OFFSET);
    }

    @Override
    public int getHeapCost() {
        return REFERENCE_COST_IN_BYTES + ARRAY_HEADER_SIZE_IN_BYTES + header.length + payload.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (!(o instanceof Data)) {
            return false;
        }

        Data data = (Data) o;
        if (getType() != data.getType()) {
            return false;
        }

        final int dataSize = dataSize();
        if (dataSize != data.dataSize()) {
            return false;
        }

        return dataSize == 0 || equals(this.header, data.toByteArray());
    }

    // Same as Arrays.equals(byte[] a, byte[] a2) but loop order is reversed.
    private static boolean equals(byte[] data1, byte[] data2) {
        if (data1 == data2) {
            return true;
        }
        if (data1 == null || data2 == null) {
            return false;
        }
        final int length = data1.length;
        if (data2.length != length) {
            return false;
        }
        for (int i = length - 1; i >= DATA_OFFSET; i--) {
            if (data1[i] != data2[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return HashUtil.MurmurHash3_x86_32(payload, 0, dataSize());
    }

    @Override
    public long hash64() {
        return HashUtil.MurmurHash3_x64_64(payload, 0, dataSize());
    }

    @Override
    public boolean isPortable() {
        return SerializationConstants.CONSTANT_TYPE_PORTABLE == getType();
    }

    @Override
    public boolean isJson() {
        return SerializationConstants.JAVASCRIPT_JSON_SERIALIZATION_TYPE == getType();
    }

    @Override
    public String toString() {
        return "HeapData{"
                + "type=" + getType()
                + ", hashCode=" + hashCode()
                + ", partitionHash=" + getPartitionHash()
                + ", totalSize=" + totalSize()
                + ", dataSize=" + dataSize()
                + ", heapCost=" + getHeapCost()
                + '}';
    }
}
