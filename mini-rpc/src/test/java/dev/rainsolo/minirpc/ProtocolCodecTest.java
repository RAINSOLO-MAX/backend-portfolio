package dev.rainsolo.minirpc;

import dev.rainsolo.minirpc.common.MessageType;
import dev.rainsolo.minirpc.protocol.RpcMessage;
import dev.rainsolo.minirpc.protocol.RpcMessageDecoder;
import dev.rainsolo.minirpc.protocol.RpcMessageEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ProtocolCodecTest {

    private static RpcMessage sample(long requestId, String traceId) {
        RpcMessage message = new RpcMessage();
        message.setType(MessageType.REQUEST);
        message.setRequestId(requestId);
        message.getHeaders().put("traceId", traceId);
        message.setBody("hello".getBytes(StandardCharsets.UTF_8));
        return message;
    }

    private static ByteBuf encode(RpcMessage message) {
        EmbeddedChannel encoder = new EmbeddedChannel(new RpcMessageEncoder());
        encoder.writeOutbound(message);
        return encoder.readOutbound();
    }

    @Test
    void roundTripPreservesHeaderAndBody() {
        EmbeddedChannel decoder = new EmbeddedChannel(new RpcMessageDecoder());
        decoder.writeInbound(encode(sample(42L, "trace-abc")));
        RpcMessage decoded = decoder.readInbound();
        assertEquals(MessageType.REQUEST, decoded.getType());
        assertEquals(42L, decoded.getRequestId());
        assertEquals("trace-abc", decoded.getHeaders().get("traceId"));
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), decoded.getBody());
    }

    @Test
    void twoFramesInOneTcpSegmentAreBothDecoded() {
        ByteBuf first = encode(sample(1L, "t1"));
        ByteBuf second = encode(sample(2L, "t2"));
        EmbeddedChannel decoder = new EmbeddedChannel(new RpcMessageDecoder());
        decoder.writeInbound(Unpooled.wrappedBuffer(first, second));

        RpcMessage one = decoder.readInbound();
        RpcMessage two = decoder.readInbound();
        assertEquals(1L, one.getRequestId());
        assertEquals("t1", one.getHeaders().get("traceId"));
        assertEquals(2L, two.getRequestId());
        assertEquals("t2", two.getHeaders().get("traceId"));
        assertNull(decoder.readInbound());
    }

    @Test
    void frameSplitAcrossManySegmentsIsReassembled() {
        ByteBuf frame = encode(sample(7L, "t7"));
        byte[] bytes = new byte[frame.readableBytes()];
        frame.getBytes(frame.readerIndex(), bytes);

        EmbeddedChannel decoder = new EmbeddedChannel(new RpcMessageDecoder());
        for (byte single : bytes) {
            decoder.writeInbound(Unpooled.wrappedBuffer(new byte[]{single}));
        }
        RpcMessage decoded = decoder.readInbound();
        assertEquals(7L, decoded.getRequestId());
        assertEquals("t7", decoded.getHeaders().get("traceId"));
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), decoded.getBody());
    }

    @Test
    void badMagicIsRejectedInsteadOfProducingAMessage() {
        ByteBuf bad = Unpooled.buffer(RpcMessageDecoder.HEADER_LENGTH);
        bad.writeShort(0xDEAD);
        bad.writeZero(RpcMessageDecoder.HEADER_LENGTH - 2);
        EmbeddedChannel decoder = new EmbeddedChannel(new RpcMessageDecoder());
        try {
            decoder.writeInbound(bad);
        } catch (Exception ignored) {
            // DecoderException is also an acceptable rejection.
        }
        assertNull(decoder.readInbound());
    }

    @Test
    void oversizedFrameIsRejected() {
        ByteBuf bad = Unpooled.buffer(RpcMessageDecoder.HEADER_LENGTH);
        bad.writeShort(RpcMessageDecoder.MAGIC).writeByte(1).writeByte(0).writeByte(0).writeByte(0);
        bad.writeLong(1L);
        bad.writeInt(0);
        bad.writeInt(RpcMessageDecoder.MAX_FRAME_LENGTH + 1);
        EmbeddedChannel decoder = new EmbeddedChannel(new RpcMessageDecoder());
        try {
            decoder.writeInbound(bad);
        } catch (Exception ignored) {
            // Expected.
        }
        assertNull(decoder.readInbound());
    }
}
