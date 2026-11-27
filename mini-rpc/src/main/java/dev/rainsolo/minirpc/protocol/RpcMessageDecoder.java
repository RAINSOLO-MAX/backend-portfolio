package dev.rainsolo.minirpc.protocol;

import dev.rainsolo.minirpc.common.MessageType;
import dev.rainsolo.minirpc.common.RpcStatus;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;

import java.util.List;

/**
 * Length-prefixed decoder. It uses absolute reads while checking the header, so a
 * partially received frame is left untouched in the buffer instead of being consumed.
 * That is what actually fixes TCP sticky/split packets.
 *
 * <pre>
 *  0  2      3      4      5      6          14        18        22
 *  | magic | ver | type | status | res | requestId | headLen | bodyLen | headers | body |
 * </pre>
 */
public final class RpcMessageDecoder extends ByteToMessageDecoder {

    public static final short MAGIC = (short) 0xCAFE;
    public static final int HEADER_LENGTH = 22;
    public static final int MAX_FRAME_LENGTH = 8 * 1024 * 1024;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < HEADER_LENGTH) {
            return;
        }
        int readerIndex = in.readerIndex();

        short magic = in.getShort(readerIndex);
        if (magic != MAGIC) {
            throw new CorruptedFrameException("Bad magic: expected 0xCAFE but got " + Integer.toHexString(magic));
        }
        int headerLength = in.getInt(readerIndex + 14);
        int bodyLength = in.getInt(readerIndex + 18);
        if (headerLength < 0 || bodyLength < 0
                || (long) HEADER_LENGTH + headerLength + bodyLength > MAX_FRAME_LENGTH) {
            in.skipBytes(in.readableBytes());
            throw new CorruptedFrameException("Illegal frame length: header=" + headerLength + ", body=" + bodyLength);
        }

        int frameLength = HEADER_LENGTH + headerLength + bodyLength;
        if (in.readableBytes() < frameLength) {
            return;
        }

        in.skipBytes(2);
        byte version = in.readByte();
        byte type = in.readByte();
        byte status = in.readByte();
        in.skipBytes(1);
        long requestId = in.readLong();
        in.readInt();
        in.readInt();
        byte[] headerBytes = new byte[headerLength];
        in.readBytes(headerBytes);
        byte[] body = new byte[bodyLength];
        in.readBytes(body);

        RpcMessage message = new RpcMessage();
        message.setVersion(version);
        message.setType(MessageType.fromCode(type));
        message.setStatus(RpcStatus.fromCode(status));
        message.setRequestId(requestId);
        message.setHeaders(Headers.decode(headerBytes));
        message.setBody(body);
        out.add(message);
    }
}
