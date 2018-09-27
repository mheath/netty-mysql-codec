package com.github.mheath.netty.codec.mysql.integration.server;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.mheath.netty.codec.mysql.ColumnCount;
import com.github.mheath.netty.codec.mysql.ColumnDefinition;
import com.github.mheath.netty.codec.mysql.ColumnFlag;
import com.github.mheath.netty.codec.mysql.ColumnType;
import com.github.mheath.netty.codec.mysql.EofResponse;
import com.github.mheath.netty.codec.mysql.Handshake;
import com.github.mheath.netty.codec.mysql.HandshakeResponse;
import com.github.mheath.netty.codec.mysql.MysqlCharacterSet;
import com.github.mheath.netty.codec.mysql.MysqlClientConnectionPacketDecoder;
import com.github.mheath.netty.codec.mysql.MysqlClientPacketDecoder;
import com.github.mheath.netty.codec.mysql.MysqlServerPacketEncoder;
import com.github.mheath.netty.codec.mysql.QueryCommand;
import com.github.mheath.netty.codec.mysql.ResultsetRow;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import com.github.mheath.netty.codec.mysql.CapabilityFlags;
import com.github.mheath.netty.codec.mysql.MysqlClientCommandPacketDecoder;
import com.github.mheath.netty.codec.mysql.OkResponse;
import org.assertj.core.api.Assertions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
public class TestServer implements AutoCloseable {

	private final int port;
	private final String user = "user";
	private final Channel channel;
	private final io.netty.channel.EventLoopGroup parentGroup;
	private final EventLoopGroup childGroup;
	private String password = "password";

	public TestServer(int port) {
		this.port = port;

		parentGroup = new NioEventLoopGroup();
		childGroup = new NioEventLoopGroup();
		final ChannelFuture channelFuture = new ServerBootstrap()
				.group(parentGroup, childGroup)
				.channel(NioServerSocketChannel.class)
				.childHandler(new ChannelInitializer<NioSocketChannel>() {
					@Override
					protected void initChannel(NioSocketChannel ch) throws Exception {
						System.out.println("Initializing child channel");
						final ChannelPipeline pipeline = ch.pipeline();
						pipeline.addLast(new MysqlServerPacketEncoder());
						pipeline.addLast(new MysqlClientConnectionPacketDecoder());
						pipeline.addLast(new ServerHandler());
					}
				})
				.bind(port);
		channel = channelFuture.channel();
		channelFuture.awaitUninterruptibly();
		assertThat(channel.isActive()).isTrue();
		System.out.println("Test MySQL server listening on port " + port);
	}

	public int getPort() {
		return port;
	}

	@Override
	public void close() {
		channel.close();
		childGroup.shutdownGracefully().awaitUninterruptibly();
		parentGroup.shutdownGracefully().awaitUninterruptibly();
	}

	public String getPassword() {
		return password;
	}

	public String getUser() {
		return user;
	}

	private class ServerHandler extends ChannelInboundHandlerAdapter {
		private byte[] salt = new byte[20];

		public ServerHandler() {
			new Random().nextBytes(salt);
		}

		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			System.out.println("Server channel active");
			final EnumSet<CapabilityFlags> capabilities = CapabilityFlags.getImplicitCapabilities();
			CapabilityFlags.setCapabilitiesAttr(ctx.channel(), capabilities);
			ctx.writeAndFlush(Handshake.builder()
					.serverVersion("5.3.1")
					.connectionId(1)
					.addAuthData(salt)
					.characterSet(MysqlCharacterSet.UTF8_BIN)
					.addCapabilities(capabilities)
					.build());
		}

		@Override
		public void channelInactive(ChannelHandlerContext ctx) throws Exception {
			System.out.println("Server channel inactive");
		}

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
			if (msg instanceof HandshakeResponse) {
				handleHandshakeResponse(ctx, (HandshakeResponse) msg);
			} else if (msg instanceof QueryCommand) {
				handleQuery(ctx, (QueryCommand) msg);
			} else {
				System.out.println("Received message: " + msg);
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
			cause.printStackTrace();
			ctx.close();
		}
	}

	private void handleHandshakeResponse(ChannelHandlerContext ctx, HandshakeResponse response) {
		System.out.println("Received handshake response");
		// TODO Validate username/password and assert database name
		ctx.pipeline().replace(MysqlClientPacketDecoder.class, "CommandPacketDecoder", new MysqlClientCommandPacketDecoder());
		ctx.writeAndFlush(OkResponse.builder().build());
	}

	private void handleQuery(ChannelHandlerContext ctx, QueryCommand query) {
		final String queryString = query.getQuery();
		System.out.println("Received query: " + queryString);

		if (isServerSettingsQuery(queryString)) {
			sendSettingsResponse(ctx, query);
		} else {
			// Generic response
			int sequenceId = query.getSequenceId();
			ctx.write(new ColumnCount(++sequenceId, 1));
			ctx.write(ColumnDefinition.builder()
					.sequenceId(++sequenceId)
					.catalog("catalog")
					.schema("schema")
					.table("table")
					.orgTable("org_table")
					.name("name")
					.orgName("org_name")
					.columnLength(10)
					.type(ColumnType.MYSQL_TYPE_DOUBLE)
					.addFlags(ColumnFlag.NUM)
					.decimals(5)
					.build());
			ctx.write(new EofResponse(++sequenceId, 0));
			ctx.write(new ResultsetRow(++sequenceId, "1"));
			ctx.writeAndFlush(new EofResponse(++sequenceId, 0));
		}
	}

	private boolean isServerSettingsQuery(String query) {
		query = query.toLowerCase();
		return query.contains("select") && !query.contains("from") && query.contains("@@");
	}

	private static Pattern SETTINGS_PATTERN = Pattern.compile("@@(\\w+)\\sAS\\s(\\w+)");

	private void sendSettingsResponse(ChannelHandlerContext ctx, QueryCommand query) {
		final Matcher matcher = SETTINGS_PATTERN.matcher(query.getQuery());

		final List<String> values = new ArrayList<>();
		int sequenceId = query.getSequenceId();

		while (matcher.find()) {
			String systemVariable = matcher.group(1);
			String fieldName = matcher.group(2);
			switch (systemVariable) {
				case "character_set_client":
				case "character_set_connection":
				case "character_set_results":
				case "character_set_server":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 12));
					values.add("utf8");
					break;
				case "collation_server":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 21));
					values.add("utf8_general_ci");
					break;
				case "init_connect":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 0));
					values.add("");
					break;
				case "interactive_timeout":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 21));
					values.add("28800");
					break;
				case "language":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 0));
					values.add("");
					break;
				case "license":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 21));
					values.add("ASLv2");
					break;
				case "lower_case_table_names":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 63));
					values.add("2");
					break;
				case "max_allowed_packet":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 63));
					values.add("4194304");
					break;
				case "net_buffer_length":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 63));
					values.add("16384");
					break;
				case "net_write_timeout":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 63));
					values.add("60");
					break;
				case "have_query_cache":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 6));
					values.add("YES");
					break;
				case "sql_mode":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 0));
					values.add("");
					break;
				case "system_time_zone":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 6));
					values.add("UTC");
					break;
				case "time_zone":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 12));
					values.add("SYSTEM");
					break;
				case "tx_isolation":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 12));
					values.add("REPEATABLE-READ");
					break;
				case "wait_timeout":
					ctx.write(newColumnDefinition(sequenceId++, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 12));
					values.add("28800");
					break;
				default:
					throw new Error("Unknown system variable " + systemVariable);
			}
		}
		ctx.write(new EofResponse(++sequenceId, 0));
		ctx.write(new ResultsetRow(++sequenceId, values.toArray(new String[values.size()])));
		ctx.writeAndFlush(new EofResponse(++sequenceId, 0));
	}

	private ColumnDefinition newColumnDefinition(int packetSequence, String name, String orgName, ColumnType columnType, int length) {
		return ColumnDefinition.builder()
				.sequenceId(packetSequence)
				.name(name)
				.orgName(orgName)
				.type(columnType)
				.columnLength(length)
				.build();
	}

}
