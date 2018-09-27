/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.github.mheath.netty.codec.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;


/**
 *
 */
public class CodecUtilsTest {

    @Test
    public void decodeLengthEncodedInteger() {
        assertThat(readLengthEncodedBytes((byte) 0)).isEqualTo(0);
        assertThat(readLengthEncodedBytes((byte) 1)).isEqualTo(1);
        assertThat(readLengthEncodedBytes((byte) 2)).isEqualTo(2);
        assertThat(readLengthEncodedBytes((byte) 0xf9)).isEqualTo(0xf9);
        assertThat(readLengthEncodedBytes((byte) 0xfa)).isEqualTo(0xfa);
        assertThat(readLengthEncodedBytes((byte) 0xfb)).isEqualTo(-1); // NUL value
        assertThat(readLengthEncodedBytes((byte) 0xfc, (byte) 0xfb, (byte) 0x00)).isEqualTo(0xfb);

    }

    @Test
    public void encodeLengthEncodedInteger() {
        assertLEI(0l, TestUtils.toBuf(0));
        assertLEI(1l, TestUtils.toBuf(1));
        assertLEI(null, TestUtils.toBuf(0xfb));
        assertLEI(0xfcl, TestUtils.toBuf(0xfc, 0xfc, 0x00));
        assertLEI(0x10000l, TestUtils.toBuf(0xfd, 0x00, 0x00, 0x01));
        assertLEI(0x1000000l, TestUtils.toBuf(0xfe, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00));
    }

    private void assertLEI(Long value, ByteBuf expected) {
        final ByteBuf buf = Unpooled.buffer();
        try {
            CodecUtils.writeLengthEncodedInt(buf, value);
            ByteBufAssert.assertThat(buf).matches(expected);
        } finally {
            buf.release();
        }
    }

    private long readLengthEncodedBytes(byte... bytes) {
        return CodecUtils.readLengthEncodedInteger(Unpooled.wrappedBuffer(bytes));
    }

    @Test
    public void enumSetVectorConversion() {
        assertEnumSetVectorConversion(
                CapabilityFlags.class,
                CapabilityFlags.CLIENT_SSL,
                CapabilityFlags.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS,
                CapabilityFlags.CLIENT_INTERACTIVE);
        assertEnumSetVectorConversion(
                ColumnFlag.class,
                ColumnFlag.BINARY,
                ColumnFlag.PRI_KEY,
                ColumnFlag.UNIQUE_KEY,
                ColumnFlag.SET);
        assertEnumSetVectorConversion(ServerStatusFlag.class);
        assertEnumSetVectorConversion(CapabilityFlags.class, CapabilityFlags.values());
    }

    public <T extends Enum<T>> void assertEnumSetVectorConversion(Class<T> type, T... enums) {
        final Set<T> set;
        if (enums.length == 0) {
            set = EnumSet.noneOf(type);
        } else {
            set = EnumSet.copyOf(Arrays.asList(enums));
        }
        final long vector = CodecUtils.toLong(set);
        final Set<T> decodedSet = CodecUtils.toEnumSet(type, vector);
        assertThat(decodedSet).contains(enums);
    }

}
