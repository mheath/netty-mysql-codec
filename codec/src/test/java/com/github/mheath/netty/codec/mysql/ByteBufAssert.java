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
import io.netty.buffer.ByteBufUtil;
import org.assertj.core.api.AbstractAssert;

/**
 *
 */
public class ByteBufAssert extends AbstractAssert<ByteBufAssert, ByteBuf> {

    public static ByteBufAssert assertThat(ByteBuf actual) {
        return new ByteBufAssert(actual);
    }

    private ByteBufAssert(ByteBuf actual) {
        super(actual, ByteBufAssert.class);
    }

    public ByteBufAssert matches(ByteBuf expected) {
        if (!ByteBufUtil.equals(actual, expected)) {
            failWithMessage(
                    "Expected the buffer:\n%s\nbut got:\n%s",
                    ByteBufUtil.prettyHexDump(expected),
                    ByteBufUtil.prettyHexDump(actual));
        }
        return this;
    }

}
