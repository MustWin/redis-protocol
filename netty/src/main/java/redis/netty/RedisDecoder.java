package redis.netty;

import java.io.IOException;

import com.google.common.base.Charsets;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferIndexFinder;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.jboss.netty.handler.codec.replay.UnreplayableOperationException;
import redis.reply.BulkReply;
import redis.reply.ErrorReply;
import redis.reply.IntegerReply;
import redis.reply.MultiBulkReply;
import redis.reply.Reply;
import redis.reply.StatusReply;

/**
 * Netty codec for Redis
 */

enum State {
}

public class RedisDecoder extends ReplayingDecoder<State> {

  public static final char CR = '\r';
  public static final char LF = '\n';
  private static final char ZERO = '0';

  // We track the current multibulk reply in the case
  // where we do not get a complete reply in a single
  // decode invocation.
  private MultiBulkReply reply;

  public static byte[] toBytes(Number length) {
    return length.toString().getBytes();
  }

  public ChannelBuffer readBytes(ChannelBuffer is) throws IOException {
    int size = readInteger(is);
    if (size == -1) {
      return null;
    }
    if (super.actualReadableBytes() < size + 2) {
      is.skipBytes(size + 2);
      throw new UnreplayableOperationException();
    }
    byte[] bytes = new byte[size];
    is.readBytes(bytes, 0, size);
    int cr = is.readByte();
    int lf = is.readByte();
    if (cr != CR || lf != LF) {
      throw new IOException("Improper line ending: " + cr + ", " + lf);
    }
    return ChannelBuffers.wrappedBuffer(bytes);
  }

  public static int readInteger(ChannelBuffer is) throws IOException {
    int size = 0;
    int sign = 1;
    int read = is.readByte();
    if (read == '-') {
      read = is.readByte();
      sign = -1;
    }
    do {
      if (read == CR) {
        if (is.readByte() == LF) {
          break;
        }
      }
      int value = read - ZERO;
      if (value >= 0 && value < 10) {
        size *= 10;
        size += value;
      } else {
        throw new IOException("Invalid character in integer");
      }
      read = is.readByte();
    } while (true);
    return size * sign;
  }

  public Reply receive(final ChannelBuffer is) throws IOException {
    int code = is.readByte();
    switch (code) {
      case StatusReply.MARKER: {
        String status = is.readBytes(is.bytesBefore(ChannelBufferIndexFinder.CRLF)).toString(Charsets.US_ASCII);
        is.skipBytes(2);
        return new StatusReply(status);
      }
      case ErrorReply.MARKER: {
        String error = is.readBytes(is.bytesBefore(ChannelBufferIndexFinder.CRLF)).toString(Charsets.US_ASCII);
        is.skipBytes(2);
        return new ErrorReply(error);
      }
      case IntegerReply.MARKER: {
        return new IntegerReply(readInteger(is));
      }
      case BulkReply.MARKER: {
        return new BulkReply(readBytes(is));
      }
      case MultiBulkReply.MARKER: {
        return decodeMultiBulkReply(is);
      }
      default: {
        throw new IOException("Unexpected character in stream: " + code);
      }
    }
  }

  @Override
  public void checkpoint() {
    super.checkpoint();
  }

  @Override
  protected Object decode(ChannelHandlerContext channelHandlerContext, Channel channel, ChannelBuffer channelBuffer, State anEnum) throws Exception {
    return receive(channelBuffer);
  }

  public MultiBulkReply decodeMultiBulkReply(ChannelBuffer is) throws IOException {
    try {
      if (reply == null) {
        reply = new MultiBulkReply(this, is);
      }
      reply.read(this, is);
      return reply;
    } finally {
      reply = null;
    }
  }
}
