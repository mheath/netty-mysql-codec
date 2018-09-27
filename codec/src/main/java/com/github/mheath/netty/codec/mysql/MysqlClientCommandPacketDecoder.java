package com.github.mheath.netty.codec.mysql;

import java.util.List;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;

/**
 *
 */
public class MysqlClientCommandPacketDecoder extends AbstractPacketDecoder implements MysqlClientPacketDecoder {

	public MysqlClientCommandPacketDecoder() {
		this(Constants.DEFAULT_MAX_PACKET_SIZE);
	}

	public MysqlClientCommandPacketDecoder(int maxPacketSize) {
		super(maxPacketSize);
	}

	@Override
	protected void decodePacket(ChannelHandlerContext ctx, int sequenceId, ByteBuf packet, List<Object> out) {
		final MysqlCharacterSet clientCharset = MysqlCharacterSet.getClientCharsetAttr(ctx.channel());

		final byte commandCode = packet.readByte();
		final Optional<Command> command = Command.findByCommandCode(commandCode);
		if (!command.isPresent()) {
			throw new DecoderException("Unknown command " + commandCode);
		}
		switch (command.get()) {
			case COM_QUERY:
				out.add(new QueryCommand(sequenceId, CodecUtils.readFixedLengthString(packet, packet.readableBytes(), clientCharset.getCharset())));
				break;
			default:
				out.add(new CommandPacket(sequenceId, command.get()));
		}
	}
}
