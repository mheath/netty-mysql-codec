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
import io.netty.channel.embedded.EmbeddedChannel;
import org.assertj.core.api.Assertions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
class MessageDecodeEncodeTester<T> {

	private EmbeddedChannel channel;
	private final List<T> messages = new ArrayList<T>();
	private final List<Consumer<T>> assertions = new ArrayList<>();
	private ByteBuf buf;

	public MessageDecodeEncodeTester<T> pipeline(EmbeddedChannel channel) {
		this.channel = channel;
		return this;
	}

	public MessageDecodeEncodeTester<T> addMessage(T message) {
		return addMessage(message, (m) -> assertThat(message.equals(m)));
	}

	public MessageDecodeEncodeTester<T> addMessage(T message, Consumer<T> assertion) {
		assertion.accept(message);
		messages.add(message);
		assertions.add(assertion);
		return this;
	}

	public MessageDecodeEncodeTester<T> byteBuf(ByteBuf buf) {
		this.buf = buf;
		return this;
	}

	@SuppressWarnings("unchecked")
	public MessageDecodeEncodeTester<T> assertDecode() {
		assertThat(channel.writeInbound(buf.copy())).isTrue();
		for (Consumer<T> assertion : assertions) {
			assertion.accept((T) channel.inboundMessages().poll());
		}
		return this;
	}

	public MessageDecodeEncodeTester<T> assertEncode() {
		for (Object msg : messages) {
			channel.writeOutbound(msg);
		}
		ByteBufAssert.assertThat(channel.readOutbound()).matches(buf);
		return this;
	}

}
