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

/**
 *
 */
public class TestUtils {

    public static ByteBuf toBuf(int... values) {
        final ByteBuf buf = Unpooled.buffer(values.length);
        for (int i = 0; i < values.length; i++) {
            buf.writeByte(values[i]);
        }
        return buf;
    }

    public static ByteBuf toBuf(String hexStream) {
        hexStream = hexStream.replaceAll("\\s+", "");
        final ByteBuf buf = Unpooled.buffer(hexStream.length() / 2);
        for (int i = 0; i < hexStream.length(); i += 2) {
            final String sub = hexStream.substring(i, i + 2);
            final int b = Integer.valueOf(sub, 16);
            buf.writeByte(b);
        }
        return buf;
    }

}
