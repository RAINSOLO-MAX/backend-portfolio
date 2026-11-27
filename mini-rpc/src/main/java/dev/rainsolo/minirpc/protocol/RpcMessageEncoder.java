package dev.rainsolo.minirpc.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public final class RpcMessageEncoder extends MessageToByteEncoder<RpcMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, RpcMessage message, ByteBuf out) {
        byte[] headerBytes = Headers.encode(message.getHeaders());
        byte[] body = message.getBody();
        out.writeShort(RpcMessageDecoder.MAGIC)
                .writeByte(message.getVersion())
                .writeByte(message.getType().getCode())
                .writeByte(message.getStatus() == null ? 0 : message.getStatus().getCode())
                .writeByte(0)
                .writeLong(message.getRequestId())
                .writeInt(headerBytes.length)
                .writeInt(body.length)
                .writeBytes(headerBytes)
                .writeBytes(body);
    }
}
