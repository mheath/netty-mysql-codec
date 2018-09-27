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
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

/**
 *
 */
public class HandshakeDecodeEncodeTest {

	private static final String EXAMPLE_SERVER_VERSION = "5.5.2-m2";
	private static final int EXAMPLE_CONNECTION_ID = 11;
	private static final Set<CapabilityFlags> EXAMPLE_CAPABILITIES = EnumSet.of(
			CapabilityFlags.CLIENT_LONG_PASSWORD,
			CapabilityFlags.CLIENT_FOUND_ROWS,
			CapabilityFlags.CLIENT_LONG_FLAG,
			CapabilityFlags.CLIENT_CONNECT_WITH_DB,
			CapabilityFlags.CLIENT_NO_SCHEMA,
			CapabilityFlags.CLIENT_COMPRESS,
			CapabilityFlags.CLIENT_ODBC,
			CapabilityFlags.CLIENT_LOCAL_FILES,
			CapabilityFlags.CLIENT_IGNORE_SPACE,
			CapabilityFlags.CLIENT_PROTOCOL_41,
			CapabilityFlags.CLIENT_INTERACTIVE,
			CapabilityFlags.CLIENT_IGNORE_SIGPIPE,
			CapabilityFlags.CLIENT_TRANSACTIONS,
			CapabilityFlags.CLIENT_RESERVED,
			CapabilityFlags.CLIENT_SECURE_CONNECTION
	);

	@Test
	public void handshake() {
		final Handshake handshake = Handshake
				.builder()
				.addCapabilities(EXAMPLE_CAPABILITIES)
				.characterSet(MysqlCharacterSet.LATIN1_SWEDISH_CI)
				.connectionId(EXAMPLE_CONNECTION_ID)
				.serverVersion(EXAMPLE_SERVER_VERSION)
				.addServerStatus(ServerStatusFlag.AUTO_COMMIT)
				.addAuthData(new byte[]{0x64, 0x76, 0x48, 0x40, 0x49, 0x2d, 0x43, 0x4a, 0x2a, 0x34, 0x64,
						0x7c, 0x63, 0x5a, 0x77, 0x6b, 0x34, 0x5e, 0x5d, 0x3a, 0x00})
				.build();

		final EmbeddedChannel embeddedChannel = new EmbeddedChannel(
				new MysqlServerConnectionPacketDecoder(),
				new MysqlServerPacketEncoder());
		CapabilityFlags.setCapabilitiesAttr(embeddedChannel, EXAMPLE_CAPABILITIES);
		new MessageDecodeEncodeTester<Handshake>()
				.pipeline(embeddedChannel)
				.byteBuf(exampleHandshakePacket())
				.addMessage(handshake)
				.assertDecode()
				.assertEncode();
	}

	private static final String EXAMPLE_WITH_PLUGIN_AUTH_SERVER_VERSION = "5.6.4-m7-log";
	private static final int EXAMPLE_WITH_PLUGIN_AUTH_CONNECTION_ID = 2646;
	private static final Set<CapabilityFlags> EXAMPLE_WITH_PLUGIN_AUTH_CAPABILITIES = EnumSet.copyOf(EXAMPLE_CAPABILITIES);

	static {
		EXAMPLE_WITH_PLUGIN_AUTH_CAPABILITIES.add(CapabilityFlags.CLIENT_SSL);
		EXAMPLE_WITH_PLUGIN_AUTH_CAPABILITIES.add(CapabilityFlags.CLIENT_MULTI_STATEMENTS);
		EXAMPLE_WITH_PLUGIN_AUTH_CAPABILITIES.add(CapabilityFlags.CLIENT_MULTI_RESULTS);
		EXAMPLE_WITH_PLUGIN_AUTH_CAPABILITIES.add(CapabilityFlags.CLIENT_PS_MULTI_RESULTS);
		EXAMPLE_WITH_PLUGIN_AUTH_CAPABILITIES.add(CapabilityFlags.CLIENT_PLUGIN_AUTH);
	}

	@Test
	public void handshakeWithClientPluginAuth() {
		final Handshake handshake = Handshake
				.builder()
				.addCapabilities(EXAMPLE_WITH_PLUGIN_AUTH_CAPABILITIES)
				.addAuthData(new byte[]{
						0x52, 0x42, 0x33, 0x76, 0x7a, 0x26, 0x47, 0x72,
						0x2b, 0x79, 0x44, 0x26, 0x2f, 0x5a, 0x5a, 0x33,
						0x30, 0x35, 0x5a, 0x47, 0x00})
				.authPluginName(Constants.MYSQL_NATIVE_PASSWORD)
				.characterSet(MysqlCharacterSet.LATIN1_SWEDISH_CI)
				.connectionId(EXAMPLE_WITH_PLUGIN_AUTH_CONNECTION_ID)
				.addServerStatus(ServerStatusFlag.AUTO_COMMIT)
				.serverVersion(EXAMPLE_WITH_PLUGIN_AUTH_SERVER_VERSION)
				.build();

		new MessageDecodeEncodeTester<Handshake>()
				.pipeline(new EmbeddedChannel(
						new MysqlServerConnectionPacketDecoder(),
						new MysqlServerPacketEncoder()))
				.byteBuf(exampleHandshakePacketWithPluginAuth())
				.addMessage(handshake)
				.assertDecode()
				.assertEncode();
	}

	@Test
	public void handshake_5_7_10() {
		final Handshake handshake = Handshake
				.builder()
				.addCapabilities(
						CapabilityFlags.CLIENT_LONG_PASSWORD,
						CapabilityFlags.CLIENT_FOUND_ROWS,
						CapabilityFlags.CLIENT_LONG_FLAG,
						CapabilityFlags.CLIENT_CONNECT_WITH_DB,
						CapabilityFlags.CLIENT_NO_SCHEMA,
						CapabilityFlags.CLIENT_COMPRESS,
						CapabilityFlags.CLIENT_ODBC,
						CapabilityFlags.CLIENT_LOCAL_FILES,
						CapabilityFlags.CLIENT_IGNORE_SPACE,
						CapabilityFlags.CLIENT_INTERACTIVE,
						CapabilityFlags.CLIENT_SSL,
						CapabilityFlags.CLIENT_IGNORE_SIGPIPE,
						CapabilityFlags.CLIENT_TRANSACTIONS,
						CapabilityFlags.CLIENT_RESERVED,
						CapabilityFlags.CLIENT_SECURE_CONNECTION,
						CapabilityFlags.CLIENT_MULTI_STATEMENTS,
						CapabilityFlags.CLIENT_MULTI_RESULTS,
						CapabilityFlags.CLIENT_PS_MULTI_RESULTS,
						CapabilityFlags.CLIENT_PLUGIN_AUTH,
						CapabilityFlags.CLIENT_CONNECT_ATTRS,
						CapabilityFlags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA,
						CapabilityFlags.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS,
						CapabilityFlags.CLIENT_SESSION_TRACK,
						CapabilityFlags.CLIENT_DEPRECATE_EOF,
						CapabilityFlags.UNKNOWN_30,
						CapabilityFlags.UNKNOWN_31
				)
				.addAuthData(new byte[]{
						0x58, 0x2f, 0x7d, 0x63, 0x0c, 0x69, 0x6a, 0x0e,
						0x6e, 0x25, 0x68, 0x3d, 0x46, 0x2e, 0x08, 0x08,
						0x55, 0x77, 0x12, 0x63, 0x00
				})
				.authPluginName(Constants.MYSQL_NATIVE_PASSWORD)
				.characterSet(MysqlCharacterSet.UTF8_GENERAL_CI)
				.connectionId(20)
				.addServerStatus(ServerStatusFlag.AUTO_COMMIT)
				.serverVersion("5.7.10")
				.build();

		new MessageDecodeEncodeTester<Handshake>()
				.pipeline(new EmbeddedChannel(
						new MysqlServerConnectionPacketDecoder(),
						new MysqlServerPacketEncoder()))
				.byteBuf(captured_5_7_10_handshake())
				.addMessage(handshake)
				.assertDecode()
				.assertEncode();
	}

	private ByteBuf exampleHandshakePacket() {
		// Source: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
		return TestUtils.toBuf(
				0x36, 0x00, 0x00, 0x00, 0x0a, 0x35, 0x2e, 0x35, 0x2e, 0x32, 0x2d, 0x6d, 0x32, 0x00, 0x0b, 0x00,
				0x00, 0x00, 0x64, 0x76, 0x48, 0x40, 0x49, 0x2d, 0x43, 0x4a, 0x00, 0xff, 0xf7, 0x08, 0x02, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0x34, 0x64,
				0x7c, 0x63, 0x5a, 0x77, 0x6b, 0x34, 0x5e, 0x5d, 0x3a, 0x00);
	}

	private ByteBuf exampleHandshakePacketWithPluginAuth() {
		// Source: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
		return TestUtils.toBuf(
				0x50, 0x00, 0x00, 0x00, 0x0a, 0x35, 0x2e, 0x36, 0x2e, 0x34, 0x2d, 0x6d, 0x37, 0x2d, 0x6c, 0x6f,
				0x67, 0x00, 0x56, 0x0a, 0x00, 0x00, 0x52, 0x42, 0x33, 0x76, 0x7a, 0x26, 0x47, 0x72, 0x00, 0xff,
				0xff, 0x08, 0x02, 0x00, 0x0f, 0x00, 0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x2b, 0x79, 0x44, 0x26, 0x2f, 0x5a, 0x5a, 0x33, 0x30, 0x35, 0x5a, 0x47, 0x00, 0x6d, 0x79,
				0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77,
				0x6f, 0x72, 0x64, 0x00);
	}

	private ByteBuf captured_5_7_10_handshake() {
		// Source: Wireshark capture
		return TestUtils
				.toBuf("4a0000000a352e372e31300014000000" +
						"582f7d630c696a0e00ffff210200ffc1" +
						"15000000000000000000006e25683d46" +
						"2e080855771263006d7973716c5f6e61" +
						"746976655f70617373776f726400");
	}

}
