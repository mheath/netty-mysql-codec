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

package com.github.mheath.netty.codec.mysql.integration.client;

import java.util.EnumSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.github.mheath.netty.codec.mysql.Constants;
import com.github.mheath.netty.codec.mysql.Handshake;
import com.github.mheath.netty.codec.mysql.HandshakeResponse;
import com.github.mheath.netty.codec.mysql.MysqlClientPacketEncoder;
import com.github.mheath.netty.codec.mysql.MysqlPacketAssert;
import com.github.mheath.netty.codec.mysql.MysqlServerPacket;
import com.github.mheath.netty.codec.mysql.MysqlServerPacketDecoder;
import com.github.mheath.netty.codec.mysql.QueryCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import com.github.mheath.netty.codec.mysql.CapabilityFlags;
import com.github.mheath.netty.codec.mysql.MysqlClientPacket;
import com.github.mheath.netty.codec.mysql.MysqlNativePasswordUtil;
import com.github.mheath.netty.codec.mysql.MysqlServerConnectionPacketDecoder;
import com.github.mheath.netty.codec.mysql.MysqlServerResultSetPacketDecoder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 *
 */
abstract class AbstractIntegrationTest {

	static String serverHost;
	static int serverPort;
	static String user;
	static String password;
	static String database;

	static SimpleClient client;

	@BeforeAll
	public static void init() {
		serverHost = getProperty("mysql.host");
		serverPort = Integer.getInteger("mysql.port", 3306);
		user = getProperty("mysql.user");
		password = getProperty("mysql.password");
		database = getProperty("mysql.database");

		client = new SimpleClient();
	}

	private static String getProperty(String property) {
		final String value = System.getProperty(property);
		assumeTrue(value != null && value.trim().length() != 0, property + " property is missing");
		return value;
	}

	@AfterAll
	public static void cleanup() {
		if (client != null) {
			client.close();
		}
	}

	protected static final EnumSet<CapabilityFlags> CLIENT_CAPABILITIES = CapabilityFlags.getImplicitCapabilities();
	static {
		CLIENT_CAPABILITIES.addAll(EnumSet.of(CapabilityFlags.CLIENT_PLUGIN_AUTH, CapabilityFlags.CLIENT_SECURE_CONNECTION, CapabilityFlags.CLIENT_CONNECT_WITH_DB));
	}

	protected static class SimpleClient implements AutoCloseable {

		private final EventLoopGroup eventLoopGroup;
		private final Bootstrap bootstrap;

		public SimpleClient() {
			eventLoopGroup = new NioEventLoopGroup();
			bootstrap = new Bootstrap();
			bootstrap.group(eventLoopGroup);
			bootstrap.channel(NioSocketChannel.class);
			bootstrap.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) throws Exception {
					CapabilityFlags.setCapabilitiesAttr(ch, CLIENT_CAPABILITIES);
					ch.pipeline().addLast(new MysqlServerConnectionPacketDecoder());
					ch.pipeline().addLast(new MysqlClientPacketEncoder());
				}
			});

		}

		public void close() {
			eventLoopGroup.shutdownGracefully();
		}

		public Connection connect() {
			return new Connection(bootstrap.connect(serverHost, serverPort));
		}

	}

	protected static class Connection implements AutoCloseable {
		private final Channel channel;
		private final BlockingQueue<MysqlServerPacket> serverPackets = new LinkedBlockingQueue<MysqlServerPacket>();

		private Handshake handshake;

		Connection(ChannelFuture connectFuture) {
			try {
				connectFuture.addListener((ChannelFutureListener) future -> {
					if (future.isSuccess()) {
						future.channel().pipeline().addLast(new ChannelInboundHandlerAdapter() {
							@Override
							public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
								if (msg instanceof Handshake) {
									CapabilityFlags.getCapabilitiesAttr(ctx.channel()).retainAll(((Handshake) msg).getCapabilities());
								}
								serverPackets.add((MysqlServerPacket) msg);
							}
						});
					}
				});
				connectFuture = connectFuture.sync();
				if (!connectFuture.isSuccess()) {
					throw new RuntimeException(connectFuture.cause());
				}
				channel = connectFuture.channel();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		MysqlServerPacket pollServer() {
			try {
				final MysqlServerPacket packet = serverPackets.poll(5, TimeUnit.SECONDS);
				if (packet == null) {
					fail("Timed out waiting for packet from server.");
				}
				return packet;
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		MysqlPacketAssert assertThatNextPacket() {
			return new MysqlPacketAssert(pollServer());
		}

		public ChannelFuture write(MysqlClientPacket packet) {
			return channel.writeAndFlush(packet).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
		}

		public ChannelFuture query(String query) {
			channel.pipeline().replace(MysqlServerPacketDecoder.class, "decoder", new MysqlServerResultSetPacketDecoder());
			return write(new QueryCommand(0, query));
		}

		public void authenticate() {
			final Handshake handshake = (Handshake) pollServer();

			final HandshakeResponse response = HandshakeResponse
					.create()
					.addCapabilities(CLIENT_CAPABILITIES)
					.username(user)
					.addAuthData(MysqlNativePasswordUtil.hashPassword(password, handshake.getAuthPluginData()))
					.database(database)
					.authPluginName(Constants.MYSQL_NATIVE_PASSWORD)
					.build();
			write(response);
			assertThatNextPacket().isOkResponse();
		}

		@Override
		public void close() {
			try {
				channel.close().sync();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

	}
}
