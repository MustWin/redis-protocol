package redis.server.netty;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.stanford.ramcloud.RAMCloud;
import edu.stanford.ramcloud.RAMCloudObject;
import static edu.stanford.ramcloud.ClientException.*;

import edu.stanford.ramcloud.RejectRules;
import edu.stanford.ramcloud.TableIterator;
import io.netty.buffer.ByteBuf;
import redis.netty4.*;
import redis.util.*;

import java.io.*;
import java.lang.reflect.Field;
import java.security.SecureRandom;
import java.util.*;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.remainderUnsigned;
import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.StatusReply.OK;
import static redis.netty4.StatusReply.QUIT;
import static redis.util.Encoding.bytesToNum;
import static redis.util.Encoding.numToBytes;

public class SimpleRedisServer implements RedisServer {
    private Kryo kryo = new Kryo();

    private final RAMCloud ramCloud;
    private final long hashTableId, keyTableID, setTableID;

    public SimpleRedisServer(RAMCloud ramCloud)
    {
      this.ramCloud = ramCloud;
      //Technically, this should only require one table, because
      //Redis shares a global keyspace, but that could get very hairy
      hashTableId = ramCloud.createTable("REDIS_RAMCLOUD_HASH");
      keyTableID = ramCloud.createTable("REDIS_RAMCLOUD_KEY");
      setTableID = ramCloud.createTable("REDIS_RAMCLOUD_SET");
      kryo.register(BytesKey.class, new BytesKeySerializer());
    }
  private static final StatusReply PONG = new StatusReply("PONG");
  private long started = now();

  private BytesKeyObjectMap<Object> data = new BytesKeyObjectMap<Object>();
  private BytesKeyObjectMap<Long> expires = new BytesKeyObjectMap<Long>();
  private static int[] mask = {128, 64, 32, 16, 8, 4, 2, 1};

  private static RedisException invalidValue() {
    return new RedisException("Operation against a key holding the wrong kind of value");
  }

  private static RedisException notInteger() {
    return new RedisException("value is not an integer or out of range");
  }

  private static RedisException notFloat() {
    return new RedisException("value is not a float or out of range");
  }

  private static RedisException notImplemented() {
        return new RedisException("method is not implemented in RamCLOUD Redis Proxy");
    }
  @SuppressWarnings("unchecked")
  private BytesKeyObjectMap<byte[]> _gethash(byte[] key0, boolean create) throws RedisException {
    Object o = _get(key0);
    if (o == null) {
      o = new BytesKeyObjectMap();
      if (create) {
        data.put(key0, o);
      }
    }
    if (!(o instanceof HashMap)) {
      throw invalidValue();
    }
    return (BytesKeyObjectMap<byte[]>) o;
  }

  @SuppressWarnings("unchecked")
  private BytesKeySet _getset(byte[] key0, boolean create) throws RedisException {
    Object o = _get(key0);
    if (o == null) {
      o = new BytesKeySet();
      if (create) {
        data.put(key0, o);
      }
    }
    if (!(o instanceof BytesKeySet)) {
      throw invalidValue();
    }
    return (BytesKeySet) o;
  }

  @SuppressWarnings("unchecked")
  private ZSet _getzset(byte[] key0, boolean create) throws RedisException {
    Object o = _get(key0);
    if (o == null) {
      o = new ZSet();
      if (create) {
        data.put(key0, o);
      }
    }
    if (!(o instanceof ZSet)) {
      throw invalidValue();
    }
    return (ZSet) o;
  }

  private Object _get(byte[] key0) {
    Object o = data.get(key0);
    if (o != null) {
      Long l = expires.get(key0);
      if (l != null) {
        if (l < now()) {
          data.remove(key0);
          return null;
        }
      }
    }
    return o;
  }

  private IntegerReply _change(byte[] key0, long delta) throws RedisException {
    Object o = _get(key0);
    if (o == null) {
      _put(key0, numToBytes(delta, false));
      return integer(delta);
    } else if (o instanceof byte[]) {
      try {
        long integer = bytesToNum((byte[]) o) + delta;
        _put(key0, numToBytes(integer, false));
        return integer(integer);
      } catch (IllegalArgumentException e) {
        throw new RedisException(e.getMessage());
      }
    } else {
      throw notInteger();
    }
  }

  private BulkReply _change(byte[] key0, double delta) throws RedisException {
    Object o = _get(key0);
    if (o == null) {
      byte[] bytes = _tobytes(delta);
      _put(key0, bytes);
      return new BulkReply(bytes);
    } else if (o instanceof byte[]) {
      try {
        double number = _todouble((byte[]) o) + delta;
        byte[] bytes = _tobytes(number);
        _put(key0, bytes);
        return new BulkReply(bytes);
      } catch (IllegalArgumentException e) {
        throw new RedisException(e.getMessage());
      }
    } else {
      throw notInteger();
    }
  }

  private static int _test(byte[] bytes, long offset) throws RedisException {
    long div = offset / 8;
    if (div > MAX_VALUE) throw notInteger();
    int i;
    if (bytes.length < div + 1) {
      i = 0;
    } else {
      int mod = (int) (offset % 8);
      int value = bytes[((int) div)] & 0xFF;
      i = value & mask[mod];
    }
    return i != 0 ? 1 : 0;
  }

  private byte[] _getbytes(byte[] aKey2) throws RedisException {
    byte[] src;
    Object o = _get(aKey2);
    if (o instanceof byte[]) {
      src = (byte[]) o;
    } else if (o != null) {
      throw invalidValue();
    } else {
      src = new byte[0];
    }
    return src;
  }

  @SuppressWarnings("unchecked")
  private List<BytesValue> _getlist(byte[] key0, boolean create) throws RedisException {
    Object o = _get(key0);
    if (o instanceof List) {
      return (List<BytesValue>) o;
    } else if (o == null) {
      if (create) {
        ArrayList<BytesValue> list = new ArrayList<BytesValue>();
        _put(key0, list);
        return list;
      } else {
        return null;
      }
    } else {
      throw invalidValue();
    }
  }

  private Object _put(byte[] key, Object value) {
    expires.remove(key);
    return data.put(key, value);
  }

  private Object _put(byte[] key, byte[] value, long expiration) {
    expires.put(key, expiration);
    return data.put(key, value);
  }

  private static boolean matches(byte[] key, byte[] pattern, int kp, int pp) {
    if (kp == key.length) {
      return pp == pattern.length || (pp == pattern.length - 1 && pattern[pp] == '*');
    } else if (pp == pattern.length) {
      return false;
    }
    byte c = key[kp];
    byte p = pattern[pp];
    switch (p) {
      case '?':
        // Always matches, move to next character in key and pattern
        return matches(key, pattern, kp + 1, pp + 1);
      case '*':
        // Matches this character than either matches end or try next character
        return matches(key, pattern, kp + 1, pp + 1) || matches(key, pattern, kp + 1, pp);
      case '\\':
        // Matches the escaped character and the rest
        return c == pattern[pp + 1] && matches(key, pattern, kp + 1, pp + 2);
      case '[':
        // Matches one of the characters and the rest
        boolean found = false;
        pp++;
        do {
          byte b = pattern[pp++];
          if (b == ']') {
            break;
          } else {
            if (b == c) found = true;
          }
        } while (true);
        return found && matches(key, pattern, kp + 1, pp);
      default:
        // This matches and the rest
        return c == p && matches(key, pattern, kp + 1, pp + 1);
    }
  }


  private static int _toposint(byte[] offset1) throws RedisException {
    long offset = bytesToNum(offset1);
    if (offset < 0 || offset > MAX_VALUE) {
      throw notInteger();
    }
    return (int) offset;
  }

  private static int _toint(byte[] offset1) throws RedisException {
    long offset = bytesToNum(offset1);
    if (offset > MAX_VALUE) {
      throw notInteger();
    }
    return (int) offset;
  }

  private static int _torange(byte[] offset1, int length) throws RedisException {
    long offset = bytesToNum(offset1);
    if (offset > MAX_VALUE) {
      throw notInteger();
    }
    if (offset < 0) {
      offset = (length + offset);
    }
    if (offset >= length) {
      offset = length - 1;
    }
    return (int) offset;
  }

    /*
  private static Random r = new SecureRandom();
  private static Field tableField;
  private static Field nextField;
  private static Field mapField;

  static {
    try {
      tableField = HashMap.class.getDeclaredField("table");
      tableField.setAccessible(true);
      nextField = Class.forName("java.util.HashMap$Entry").getDeclaredField("next");
      nextField.setAccessible(true);
      mapField = HashSet.class.getDeclaredField("map");
      mapField.setAccessible(true);
    } catch (Exception e) {
      e.printStackTrace();
      tableField = null;
      nextField = null;
    }
  }
  */

  private static RedisException noSuchKey() {
    return new RedisException("no such key");
  }

  private long now() {
    return System.currentTimeMillis();
  }

  /**
   * Append a value to a key
   * String
   *
   * @param key0
   * @param value1
   * @return IntegerReply
   */
  @Override
  public IntegerReply append(byte[] key0, byte[] value1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Count set bits in a string
   * String
   *
   * @param key0
   * @param start1
   * @param end2
   * @return IntegerReply
   */
  @Override
  public IntegerReply bitcount(byte[] key0, byte[] start1, byte[] end2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Perform bitwise operations between strings
   * String
   *
   * @param operation0
   * @param destkey1
   * @param key2
   * @return IntegerReply
   */
  @Override
  public IntegerReply bitop(byte[] operation0, byte[] destkey1, byte[][] key2) throws RedisException {
    throw notImplemented();
  }

  enum BitOp {AND, OR, XOR, NOT}

  /**
   * Decrement the integer value of a key by one
   * String
   *
   * @param key0
   * @return IntegerReply
   */
  @Override
  public IntegerReply decr(byte[] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Decrement the integer value of a key by the given number
   * String
   *
   * @param key0
   * @param decrement1
   * @return IntegerReply
   */
  @Override
  public IntegerReply decrby(byte[] key0, byte[] decrement1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Get the value of a key
   * String
   *
   * @param key0
   * @return BulkReply
   */
  @Override
  public BulkReply get(byte[] key0) throws RedisException {
      try {
          final RAMCloudObject read = ramCloud.read(keyTableID, key0);
          return new BulkReply(read.getValueBytes());

      } catch (Exception ObjectNotFoundException) {
          return NIL_REPLY;
      }
  }

  /**
   * Returns the bit value at offset in the string value stored at key
   * String
   *
   * @param key0
   * @param offset1
   * @return IntegerReply
   */
  @Override
  public IntegerReply getbit(byte[] key0, byte[] offset1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Get a substring of the string stored at a key
   * String
   *
   * @param key0
   * @param start1
   * @param end2
   * @return BulkReply
   */
  @Override
  public BulkReply getrange(byte[] key0, byte[] start1, byte[] end2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Set the string value of a key and return its old value
   * String
   *
   * @param key0
   * @param value1
   * @return BulkReply
   */
  @Override
  public BulkReply getset(byte[] key0, byte[] value1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Increment the integer value of a key by one
   * String
   *
   * @param key0
   * @return IntegerReply
   */
  @Override
  public IntegerReply incr(byte[] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Increment the integer value of a key by the given amount
   * String
   *
   * @param key0
   * @param increment1
   * @return IntegerReply
   */
  @Override
  public IntegerReply incrby(byte[] key0, byte[] increment1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Increment the float value of a key by the given amount
   * String
   *
   * @param key0
   * @param increment1
   * @return BulkReply
   */
  @Override
  public BulkReply incrbyfloat(byte[] key0, byte[] increment1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Get the values of all the given keys
   * String
   *
   * @param key0
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply mget(byte[][] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Set multiple keys to multiple values
   * String
   *
   * @param key_or_value0
   * @return StatusReply
   */
  @Override
  public StatusReply mset(byte[][] key_or_value0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Set multiple keys to multiple values, only if none of the keys exist
   * String
   *
   * @param key_or_value0
   * @return IntegerReply
   */
  @Override
  public IntegerReply msetnx(byte[][] key_or_value0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Set the value and expiration in milliseconds of a key
   * String
   *
   * @param key0
   * @param milliseconds1
   * @param value2
   * @return Reply
   */
  @Override
  public Reply psetex(byte[] key0, byte[] milliseconds1, byte[] value2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Set the string value of a key
   * String
   *
   * @param key0
   * @param value1
   * @return StatusReply
   */
  @Override
  public StatusReply set(byte[] key0, byte[] value1) throws RedisException {
      ramCloud.write(keyTableID, key0, value1, null);
      return OK;
  }

  /**
   * Sets or clears the bit at offset in the string value stored at key
   * String
   *
   * @param key0
   * @param offset1
   * @param value2
   * @return IntegerReply
   */
  @Override
  public IntegerReply setbit(byte[] key0, byte[] offset1, byte[] value2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Set the value and expiration of a key
   * String
   *
   * @param key0
   * @param seconds1
   * @param value2
   * @return StatusReply
   */
  @Override
  public StatusReply setex(byte[] key0, byte[] seconds1, byte[] value2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Set the value of a key, only if the key does not exist
   * String
   *
   * @param key0
   * @param value1
   * @return IntegerReply
   */
  @Override
  public IntegerReply setnx(byte[] key0, byte[] value1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Overwrite part of a string at key starting at the specified offset
   * String
   *
   * @param key0
   * @param offset1
   * @param value2
   * @return IntegerReply
   */
  @Override
  public IntegerReply setrange(byte[] key0, byte[] offset1, byte[] value2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Get the length of the value stored in a key
   * String
   *
   * @param key0
   * @return IntegerReply
   */
  @Override
  public IntegerReply strlen(byte[] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Authenticate to the server
   * Connection
   *
   * @param password0
   * @return StatusReply
   */
  public StatusReply auth(byte[] password0) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Echo the given string
   * Connection
   *
   * @param message0
   * @return BulkReply
   */
  @Override
  public BulkReply echo(byte[] message0) throws RedisException {
    return new BulkReply(message0);
  }

  /**
   * Ping the server
   * Connection
   *
   * @return StatusReply
   */
  @Override
  public StatusReply ping() throws RedisException {
    return PONG;
  }

  /**
   * Close the connection
   * Connection
   *
   * @return StatusReply
   */
  @Override
  public StatusReply quit() throws RedisException {
    return QUIT;
  }

  /**
   * Change the selected database for the current connection
   * Connection
   *
   * @param index0
   * @return StatusReply
   */
  @Override
  public StatusReply select(byte[] index0) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Asynchronously rewrite the append-only file
   * Server
   *
   * @return StatusReply
   */
  @Override
  public StatusReply bgrewriteaof() throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Asynchronously save the dataset to disk
   * Server
   *
   * @return StatusReply
   */
  @Override
  public StatusReply bgsave() throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Kill the connection of a client
   * Server
   *
   * @param ip_port0
   * @return Reply
   */
  @Override
  public Reply client_kill(byte[] ip_port0) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Get the list of client connections
   * Server
   *
   * @return Reply
   */
  @Override
  public Reply client_list() throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Get the current connection name
   * Server
   *
   * @return Reply
   */
  @Override
  public Reply client_getname() throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Set the current connection name
   * Server
   *
   * @param connection_name0
   * @return Reply
   */
  @Override
  public Reply client_setname(byte[] connection_name0) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Get the value of a configuration parameter
   * Server
   *
   * @param parameter0
   * @return Reply
   */
  @Override
  public Reply config_get(byte[] parameter0) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Set a configuration parameter to the given value
   * Server
   *
   * @param parameter0
   * @param value1
   * @return Reply
   */
  @Override
  public Reply config_set(byte[] parameter0, byte[] value1) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Reset the stats returned by INFO
   * Server
   *
   * @return Reply
   */
  @Override
  public Reply config_resetstat() throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Return the number of keys in the selected database
   * Server
   *
   * @return IntegerReply
   */
  @Override
  public IntegerReply dbsize() throws RedisException {
    return integer(data.size());
  }

  /**
   * Get debugging information about a key
   * Server
   *
   * @param key0
   * @return Reply
   */
  @Override
  public Reply debug_object(byte[] key0) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Make the server crash
   * Server
   *
   * @return Reply
   */
  @Override
  public Reply debug_segfault() throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Remove all keys from all databases
   * Server
   *
   * @return StatusReply
   */
  @Override
  public StatusReply flushall() throws RedisException {
    long[] tables = {hashTableId, keyTableID, setTableID};
    for (int i = 0; i < tables.length; i++) {
      final TableIterator tableIterator = ramCloud.getTableIterator(tables[i]);
      while (tableIterator.hasNext())
      {
        ramCloud.remove(tables[i], tableIterator.next().getKeyBytes());
      }
    }
    return OK;
  }

  /**
   * Remove all keys from the current database
   * Server
   *
   * @return StatusReply
   */
  @Override
  public StatusReply flushdb() throws RedisException {
    long[] tables = {hashTableId, keyTableID, setTableID};
    for (int i = 0; i < tables.length; i++) {
      final TableIterator tableIterator = ramCloud.getTableIterator(tables[i]);
      while (tableIterator.hasNext())
      {
        ramCloud.remove(tables[i], tableIterator.next().getKeyBytes());
      }
    }
    return OK;
  }

  /**
   * Get information and statistics about the server
   * Server
   *
   * @return BulkReply
   */
  @Override
  public BulkReply info(byte[] section) throws RedisException {
    StringBuilder sb = new StringBuilder();
    sb.append("redis_version:2.6.0\n");
    sb.append("uptime:").append(now() - started).append("\n");
    return new BulkReply(sb.toString().getBytes());
  }

  /**
   * Get the UNIX time stamp of the last successful save to disk
   * Server
   *
   * @return IntegerReply
   */
  @Override
  public IntegerReply lastsave() throws RedisException {
    return integer(-1);
  }

  /**
   * Listen for all requests received by the server in real time
   * Server
   *
   * @return Reply
   */
  @Override
  public Reply monitor() throws RedisException {
    // TODO: Blocking
    return null;
  }

  /**
   * Synchronously save the dataset to disk
   * Server
   *
   * @return Reply
   */
  @Override
  public Reply save() throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Synchronously save the dataset to disk and then shut down the server
   * Server
   *
   * @param NOSAVE0
   * @param SAVE1
   * @return StatusReply
   */
  @Override
  public StatusReply shutdown(byte[] NOSAVE0, byte[] SAVE1) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Make the server a slave of another instance, or promote it as master
   * Server
   *
   * @param host0
   * @param port1
   * @return StatusReply
   */
  @Override
  public StatusReply slaveof(byte[] host0, byte[] port1) throws RedisException {
    // TODO
    return null;
  }

  /**
   * Manages the Redis slow queries log
   * Server
   *
   * @param subcommand0
   * @param argument1
   * @return Reply
   */
  @Override
  public Reply slowlog(byte[] subcommand0, byte[] argument1) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Internal command used for replication
   * Server
   *
   * @return Reply
   */
  @Override
  public Reply sync() throws RedisException {
    // TODO: Blocking
    return null;
  }

  /**
   * Return the current server time
   * Server
   *
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply time() throws RedisException {
    long millis = System.currentTimeMillis();
    long seconds = millis / 1000;
    Reply[] replies = new Reply[]{
            new BulkReply(numToBytes(seconds)),
            new BulkReply(numToBytes((millis - seconds * 1000) * 1000))
    };
    return new MultiBulkReply(replies);
  }

  /**
   * Remove and get the first element in a list, or block until one is available
   * List
   *
   * @param key0
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply blpop(byte[][] key0) throws RedisException {
    // TODO: Blocking
    return null;
  }

  /**
   * Remove and get the last element in a list, or block until one is available
   * List
   *
   * @param key0
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply brpop(byte[][] key0) throws RedisException {
    // TODO: Blocking
    return null;
  }

  /**
   * Pop a value from a list, push it to another list and return it; or block until one is available
   * List
   *
   * @param source0
   * @param destination1
   * @param timeout2
   * @return BulkReply
   */
  @Override
  public BulkReply brpoplpush(byte[] source0, byte[] destination1, byte[] timeout2) throws RedisException {
    // TODO: Blocking
    return null;
  }

  /**
   * Get an element from a list by its index
   * List
   *
   * @param key0
   * @param index1
   * @return BulkReply
   */
  @SuppressWarnings("unchecked")
  @Override
  public BulkReply lindex(byte[] key0, byte[] index1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Insert an element before or after another element in a list
   * List
   *
   * @param key0
   * @param where1
   * @param pivot2
   * @param value3
   * @return IntegerReply
   */
  @Override
  public IntegerReply linsert(byte[] key0, byte[] where1, byte[] pivot2, byte[] value3) throws RedisException {
    throw notImplemented();
  }

  enum Where {BEFORE, AFTER}

  /**
   * Get the length of a list
   * List
   *
   * @param key0
   * @return IntegerReply
   */
  @Override
  public IntegerReply llen(byte[] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Remove and get the first element in a list
   * List
   *
   * @param key0
   * @return BulkReply
   */
  @Override
  public BulkReply lpop(byte[] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Prepend one or multiple values to a list
   * List
   *
   * @param key0
   * @param value1
   * @return IntegerReply
   */
  @Override
  public IntegerReply lpush(byte[] key0, byte[][] value1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Prepend a value to a list, only if the list exists
   * List
   *
   * @param key0
   * @param value1
   * @return IntegerReply
   */
  @Override
  public IntegerReply lpushx(byte[] key0, byte[] value1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Get a range of elements from a list
   * List
   *
   * @param key0
   * @param start1
   * @param stop2
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply lrange(byte[] key0, byte[] start1, byte[] stop2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Remove elements from a list
   * List
   *
   * @param key0
   * @param count1
   * @param value2
   * @return IntegerReply
   */
  @Override
  public IntegerReply lrem(byte[] key0, byte[] count1, byte[] value2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Set the value of an element in a list by its index
   * List
   *
   * @param key0
   * @param index1
   * @param value2
   * @return StatusReply
   */
  @Override
  public StatusReply lset(byte[] key0, byte[] index1, byte[] value2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Trim a list to the specified range
   * List
   *
   * @param key0
   * @param start1
   * @param stop2
   * @return StatusReply
   */
  @Override
  public StatusReply ltrim(byte[] key0, byte[] start1, byte[] stop2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Remove and get the last element in a list
   * List
   *
   * @param key0
   * @return BulkReply
   */
  @Override
  public BulkReply rpop(byte[] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Remove the last element in a list, append it to another list and return it
   * List
   *
   * @param source0
   * @param destination1
   * @return BulkReply
   */
  @Override
  public BulkReply rpoplpush(byte[] source0, byte[] destination1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Append one or multiple values to a list
   * List
   *
   * @param key0
   * @param value1
   * @return IntegerReply
   */
  @Override
  public IntegerReply rpush(byte[] key0, byte[][] value1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Append a value to a list, only if the list exists
   * List
   *
   * @param key0
   * @param value1
   * @return IntegerReply
   */
  @Override
  public IntegerReply rpushx(byte[] key0, byte[] value1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Delete a key
   * Generic
   *
   * @param key0
   * @return IntegerReply
   */
  @Override
  public IntegerReply del(byte[][] key0) throws RedisException {
    int total = 0;
    for (byte[] bytes : key0) {
      try {
        ramCloud.remove(keyTableID, bytes, null);
        total++;
      }
      catch (Exception ObjectNotFoundException) {

      }
    }
    return integer(total);
  }

  /**
   * Return a serialized version of the value stored at the specified key.
   * Generic
   *
   * @param key0
   * @return BulkReply
   */
  @Override
  public BulkReply dump(byte[] key0) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Determine if a key exists
   * Generic
   *
   * @param key0
   * @return IntegerReply
   */
  @Override
  public IntegerReply exists(byte[] key0) throws RedisException {
    try {
      ramCloud.read(keyTableID, key0, null);
      return integer(1);
    }
    catch (Exception ObjectNotFoundException) {
      return integer(0);
    }
  }

  /**
   * Set a key's time to live in seconds
   * Generic
   *
   * @param key0
   * @param seconds1
   * @return IntegerReply
   */
  @Override
  public IntegerReply expire(byte[] key0, byte[] seconds1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Set the expiration for a key as a UNIX timestamp
   * Generic
   *
   * @param key0
   * @param timestamp1
   * @return IntegerReply
   */
  @Override
  public IntegerReply expireat(byte[] key0, byte[] timestamp1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Find all keys matching the given pattern
   * Generic
   *
   * @param pattern0
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply keys(byte[] pattern0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Atomically transfer a key from a Redis instance to another one.
   * Generic
   *
   * @param host0
   * @param port1
   * @param key2
   * @param destination_db3
   * @param timeout4
   * @return StatusReply
   */
  @Override
  public StatusReply migrate(byte[] host0, byte[] port1, byte[] key2, byte[] destination_db3, byte[] timeout4) throws RedisException {
    // TODO: Multiserver
    return null;
  }

  /**
   * Move a key to another database
   * Generic
   *
   * @param key0
   * @param db1
   * @return IntegerReply
   */
  @Override
  public IntegerReply move(byte[] key0, byte[] db1) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Inspect the internals of Redis objects
   * Generic
   *
   * @param subcommand0
   * @param arguments1
   * @return Reply
   */
  @Override
  public Reply object(byte[] subcommand0, byte[][] arguments1) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Remove the expiration from a key
   * Generic
   *
   * @param key0
   * @return IntegerReply
   */
  @Override
  public IntegerReply persist(byte[] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Set a key's time to live in milliseconds
   * Generic
   *
   * @param key0
   * @param milliseconds1
   * @return IntegerReply
   */
  @Override
  public IntegerReply pexpire(byte[] key0, byte[] milliseconds1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Set the expiration for a key as a UNIX timestamp specified in milliseconds
   * Generic
   *
   * @param key0
   * @param milliseconds_timestamp1
   * @return IntegerReply
   */
  @Override
  public IntegerReply pexpireat(byte[] key0, byte[] milliseconds_timestamp1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Get the time to live for a key in milliseconds
   * Generic
   *
   * @param key0
   * @return IntegerReply
   */
  @Override
  public IntegerReply pttl(byte[] key0) throws RedisException {
    throw notImplemented();
  }

    @Override
    public BulkReply randomkey() throws RedisException {
        return null;
    }


    /**
   * Return a random key from the keyspace
   * Generic
   *
   * @return BulkReply
   */
  /*
  @Override
  public BulkReply randomkey() throws RedisException {
    // This implementation mirrors that of Redis. I'm not
    // sure I believe that this is a great algorithm but
    // it beats the alternatives that are very inefficient.
    if (tableField != null) {
      int size = data.size();
      if (size == 0) {
        return NIL_REPLY;
      }
      try {
        BytesKey key = getRandomKey(data);
        return new BulkReply(key.getBytes());
      } catch (Exception e) {
        throw new RedisException(e);
      }
    }
    return null;
  }
    */

  /*
  private BytesKey getRandomKey(Map data1) throws IllegalAccessException {
    Map.Entry[] table = (Map.Entry[]) tableField.get(data1);
    int length = table.length;
    Map.Entry entry;
    do {
      entry = table[r.nextInt(length)];
    } while (entry == null);

    int entries = 0;
    Map.Entry current = entry;
    do {
      entries++;
      current = (Map.Entry) nextField.get(current);
    } while (current != null);
    int choose = r.nextInt(entries);
    current = entry;
    while (choose-- != 0) current = (Map.Entry) nextField.get(current);
    return (BytesKey) current.getKey();
  }
  */

  /**
   * Rename a key
   * Generic
   *
   * @param key0
   * @param newkey1
   * @return StatusReply
   */
  @Override
  public StatusReply rename(byte[] key0, byte[] newkey1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Rename a key, only if the new key does not exist
   * Generic
   *
   * @param key0
   * @param newkey1
   * @return IntegerReply
   */
  @Override
  public IntegerReply renamenx(byte[] key0, byte[] newkey1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Create a key using the provided serialized value, previously obtained using DUMP.
   * Generic
   *
   * @param key0
   * @param ttl1
   * @param serialized_value2
   * @return StatusReply
   */
  @Override
  public StatusReply restore(byte[] key0, byte[] ttl1, byte[] serialized_value2) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Sort the elements in a list, set or sorted set
   * Generic
   * <p/>
   * SORT key [BY pattern]
   * [LIMIT offset count]
   * [GET pattern [GET pattern ...]]
   * [ASC|DESC]
   * [ALPHA]
   * [STORE destination]
   *
   * @param key0
   * @param pattern1_offset_or_count2_pattern3
   *
   * @return Reply
   */
  @Override
  public Reply sort(byte[] key0, byte[][] pattern1_offset_or_count2_pattern3) throws RedisException {
    // TODO
    return null;
  }

  /**
   * Get the time to live for a key
   * Generic
   *
   * @param key0
   * @return IntegerReply
   */
  @Override
  public IntegerReply ttl(byte[] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Determine the type stored at key
   * Generic
   *
   * @param key0
   * @return StatusReply
   */
  @Override
  public StatusReply type(byte[] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Forget about all watched keys
   * Transactions
   *
   * @return StatusReply
   */
  @Override
  public StatusReply unwatch() throws RedisException {
    // TODO: Transactions
    return null;
  }

  /**
   * Watch the given keys to determine execution of the MULTI/EXEC block
   * Transactions
   *
   * @param key0
   * @return StatusReply
   */
  @Override
  public StatusReply watch(byte[][] key0) throws RedisException {
    // TODO: Transactions
    return null;
  }

  /**
   * Execute a Lua script server side
   * Scripting
   *
   * @param script0
   * @param numkeys1
   * @param key2
   * @return Reply
   */
  @Override
  public Reply eval(byte[] script0, byte[] numkeys1, byte[][] key2) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Execute a Lua script server side
   * Scripting
   *
   * @param sha10
   * @param numkeys1
   * @param key2
   * @return Reply
   */
  @Override
  public Reply evalsha(byte[] sha10, byte[] numkeys1, byte[][] key2) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Check existence of scripts in the script cache.
   * Scripting
   *
   * @param script0
   * @return Reply
   */
  @Override
  public Reply script_exists(byte[][] script0) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Remove all the scripts from the script cache.
   * Scripting
   *
   * @return Reply
   */
  @Override
  public Reply script_flush() throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Kill the script currently in execution.
   * Scripting
   *
   * @return Reply
   */
  @Override
  public Reply script_kill() throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Load the specified Lua script into the script cache.
   * Scripting
   *
   * @param script0
   * @return Reply
   */
  @Override
  public Reply script_load(byte[] script0) throws RedisException {
    throw new RedisException("Not supported");
  }

  /**
   * Delete one or more hash fields
   * Hash
   *
   * @param key0
   * @param field1
   * @return IntegerReply
   */
  @Override
  public IntegerReply hdel(byte[] key0, byte[][] field1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Determine if a hash field exists
   * Hash
   *
   * @param key0
   * @param field1
   * @return IntegerReply
   */
  @Override
  public IntegerReply hexists(byte[] key0, byte[] field1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Get the value of a hash field
   * Hash
   *
   * @param key0
   * @param field1
   * @return BulkReply
   */
  @Override
  public BulkReply hget(byte[] key0, byte[] field1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Get all the fields and values in a hash
   * Hash
   *
   * @param key0
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply hgetall(byte[] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Increment the integer value of a hash field by the given number
   * Hash
   *
   * @param key0
   * @param field1
   * @param increment2
   * @return IntegerReply
   */
  @Override
  public IntegerReply hincrby(byte[] key0, byte[] field1, byte[] increment2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Increment the float value of a hash field by the given amount
   * Hash
   *
   * @param key0
   * @param field1
   * @param increment2
   * @return BulkReply
   */
  @Override
  public BulkReply hincrbyfloat(byte[] key0, byte[] field1, byte[] increment2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Get all the fields in a hash
   * Hash
   *
   * @param key0
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply hkeys(byte[] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Get the number of fields in a hash
   * Hash
   *
   * @param key0
   * @return IntegerReply
   */
  @Override
  public IntegerReply hlen(byte[] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Get the values of all the given hash fields
   * Hash
   *
   * @param key0
   * @param field1
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply hmget(byte[] key0, byte[][] field1) throws RedisException {
      BytesKeyObjectMap<byte[]> hash;
      try {
          final RAMCloudObject read = ramCloud.read(hashTableId, key0);
          final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(read.getValueBytes());
          final Input input = new Input(byteArrayInputStream);
          hash = kryo.readObject(input, BytesKeyObjectMap.class);


      } catch (Exception ObjectNotFoundException) {
          hash = new BytesKeyObjectMap();
      }
      int length = field1.length;
      Reply[] replies = new Reply[length];
      for (int i = 0; i < length; i++) {
          byte[] bytes = hash.get(field1[i]);
          if (bytes == null) {
              replies[i] = NIL_REPLY;
          } else {
              replies[i] = new BulkReply(bytes);
          }
      }
      return new MultiBulkReply(replies);
  }

  /**
   * Set multiple hash fields to multiple values
   * Hash
   *
   * @param key0
   * @param field_or_value1
   * @return StatusReply
   */
  @Override
  public StatusReply hmset(byte[] key0, byte[][] field_or_value1) throws RedisException {
      BytesKeyObjectMap<byte[]> hash;


      if (field_or_value1.length % 2 != 0) {
          throw new RedisException("wrong number of arguments for HMSET");
      }
      for (int retries = 5; retries > 0; retries--) {
          RejectRules rules = new RejectRules();
          try {
              final RAMCloudObject read = ramCloud.read(hashTableId, key0);
              final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(read.getValueBytes());
              final Input input = new Input(byteArrayInputStream);
              hash = kryo.readObject(input, BytesKeyObjectMap.class);
              for (int i = 0; i < field_or_value1.length; i += 2) {
                  hash.put(field_or_value1[i], field_or_value1[i + 1]);
              }
              rules.rejectIfVersionNeGiven(true);
              rules.setGivenVersion(read.getVersion());

          } catch (Exception ObjectNotFoundException) {
              hash = new BytesKeyObjectMap();
              for (int i = 0; i < field_or_value1.length; i += 2) {
                  hash.put(field_or_value1[i], field_or_value1[i + 1]);
              }
          }
          ByteArrayOutputStream outStream = new ByteArrayOutputStream();
          Output output = new Output(outStream);
          kryo.writeObject(output, hash);
          output.flush();
          try {
              ramCloud.write(hashTableId, key0, outStream.toByteArray(), rules);
          }
          catch (WrongVersionException e)
          {
              continue;
          }
          return OK;
      }
      throw new RedisException("Reject Rules failed after retries");
  }

  /**
   * Set the string value of a hash field
   * Hash
   *
   * @param key0
   * @param field1
   * @param value2
   * @return IntegerReply
   */
  @Override
  public IntegerReply hset(byte[] key0, byte[] field1, byte[] value2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Set the value of a hash field, only if the field does not exist
   * Hash
   *
   * @param key0
   * @param field1
   * @param value2
   * @return IntegerReply
   */
  @Override
  public IntegerReply hsetnx(byte[] key0, byte[] field1, byte[] value2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Get all the values in a hash
   * Hash
   *
   * @param key0
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply hvals(byte[] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Post a message to a channel
   * Pubsub
   *
   * @param channel0
   * @param message1
   * @return IntegerReply
   */
  @Override
  public IntegerReply publish(byte[] channel0, byte[] message1) throws RedisException {
    // TODO: Pubsub
    return null;
  }

  /**
   * Add one or more members to a set
   * Set
   *
   * @param key0
   * @param member1
   * @return IntegerReply
   */
  @Override
  public IntegerReply sadd(byte[] key0, byte[][] member1) throws RedisException {

      //BytesKeySet set = _getset(key0, true);
      BytesKeySet set;
      int total = 0;
      for (int retries = 5; retries > 0; retries--) {
          RejectRules rules = new RejectRules();
          try {
              final RAMCloudObject read = ramCloud.read(setTableID, key0);
              final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(read.getValueBytes());
              final Input input = new Input(byteArrayInputStream);
              set = kryo.readObject(input, BytesKeySet.class);
              for (byte[] bytes : member1) {
                  if (set.add(bytes)) total++;
              }
              rules.rejectIfVersionNeGiven(true);
              rules.setGivenVersion(read.getVersion());

          } catch (Exception ObjectNotFoundException) {
              set = new BytesKeySet();
              for (byte[] bytes : member1) {
                  if (set.add(bytes)) total++;
              }
          }
          ByteArrayOutputStream outStream = new ByteArrayOutputStream();
          Output output = new Output(outStream);
          kryo.writeObject(output, set);
          output.flush();
          try {
              ramCloud.write(setTableID, key0, outStream.toByteArray(), rules);
          }
          catch (WrongVersionException e)
          {
              continue;
          }
          return integer(total);
      }
      throw new RedisException("Reject Rules failed after retries");
  }

  /**
   * Get the number of members in a set
   * Set
   *
   * @param key0
   * @return IntegerReply
   */
  @Override
  public IntegerReply scard(byte[] key0) throws RedisException {
    try {
      final RAMCloudObject read = ramCloud.read(setTableID, key0);
      final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(read.getValueBytes());
      final Input input = new Input(byteArrayInputStream);
      BytesKeySet set = kryo.readObject(input, BytesKeySet.class);
      return integer(set.size());

    } catch (Exception ObjectNotFoundException) {
      return integer(0);
    }
  }

  /**
   * Subtract multiple sets
   * Set
   *
   * @param key0
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply sdiff(byte[][] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Subtract multiple sets and store the resulting set in a key
   * Set
   *
   * @param destination0
   * @param key1
   * @return IntegerReply
   */
  @Override
  public IntegerReply sdiffstore(byte[] destination0, byte[][] key1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Intersect multiple sets
   * Set
   *
   * @param key0
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply sinter(byte[][] key0) throws RedisException {
    throw notImplemented();
  }

  private BytesKeySet _sinter(byte[][] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Intersect multiple sets and store the resulting set in a key
   * Set
   *
   * @param destination0
   * @param key1
   * @return IntegerReply
   */
  @Override
  public IntegerReply sinterstore(byte[] destination0, byte[][] key1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Determine if a given value is a member of a set
   * Set
   *
   * @param key0
   * @param member1
   * @return IntegerReply
   */
  @Override
  public IntegerReply sismember(byte[] key0, byte[] member1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Get all the members in a set
   * Set
   *
   * @param key0
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply smembers(byte[] key0) throws RedisException {

      try {
          final RAMCloudObject read = ramCloud.read(setTableID, key0);
          final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(read.getValueBytes());
          final Input input = new Input(byteArrayInputStream);
          BytesKeySet set = kryo.readObject(input, BytesKeySet.class);
          return _setreply(set);

      }
      catch (ObjectDoesntExistException e)
      {
        return _setreply(new BytesKeySet());
      }
  }

  private MultiBulkReply _setreply(BytesKeySet set) {
    Reply[] replies = new Reply[set.size()];
    int i = 0;
    for (BytesKey value : set) {
      replies[i++] = new BulkReply(value.getBytes());
    }
    return new MultiBulkReply(replies);
  }

  /**
   * Move a member from one set to another
   * Set
   *
   * @param source0
   * @param destination1
   * @param member2
   * @return IntegerReply
   */
  @Override
  public IntegerReply smove(byte[] source0, byte[] destination1, byte[] member2) throws RedisException {
    throw notImplemented();
  }

    @Override
    public BulkReply spop(byte[] key0) throws RedisException {
        return null;
    }

    /**
   * Remove and return a random member from a set
   * Set
   *
   * @param key0
   * @return BulkReply
   */
  /*
  @Override
  public BulkReply spop(byte[] key0) throws RedisException {
    if (mapField == null || tableField == null) {
      throw new RedisException("Not supported");
    }
    BytesKeySet set = _getset(key0, false);
    if (set.size() == 0) return NIL_REPLY;
    try {
      BytesKey key = getRandomKey((Map) mapField.get(set));
      set.remove(key);
      return new BulkReply(key.getBytes());
    } catch (IllegalAccessException e) {
      throw new RedisException("Not supported");
    }
  }

*/
  /**
   * Get a random member from a set
   * Set
   *
   * @param key0
   * @return BulkReply
   */
  /*
  @Override
  public Reply srandmember(byte[] key0, byte[] count1) throws RedisException {
    if (mapField == null || tableField == null) {
      throw new RedisException("Not supported");
    }
    BytesKeySet set = _getset(key0, false);
    int size = set.size();
    try {
      if (count1 == null) {
        if (size == 0) return NIL_REPLY;
        BytesKey key = getRandomKey((Map) mapField.get(set));
        return new BulkReply(key.getBytes());
      } else {
        int count = _toint(count1);
        int distinct = count < 0 ? -1 : 1;
        count *= distinct;
        if (count > size && distinct > 0) count = size;
        Reply[] replies = new Reply[count];
        Set<BytesKey> found;
        if (distinct > 0) {
          found = new HashSet<BytesKey>(count);
        } else {
          found = null;
        }
        for (int i = 0; i < count; i++) {
          BytesKey key;
          do {
            key = getRandomKey((Map) mapField.get(set));
          } while (found != null && !found.add(key));
          replies[i] = new BulkReply(key.getBytes());
        }
        return new MultiBulkReply(replies);
      }
    } catch (IllegalAccessException e) {
      throw new RedisException("Not supported");
    }
  }

*/
  /**
   * Remove one or more members from a set
   * Set
   *
   * @param key0
   * @param member1
   * @return IntegerReply
   */
  @Override
  public IntegerReply srem(byte[] key0, byte[][] member1) throws RedisException {
    BytesKeySet set = _getset(key0, false);
    int total = 0;
    for (byte[] member : member1) {
      if (set.remove(member)) {
        total++;
      }
    }
    return new IntegerReply(total);
  }

  /**
   * Add multiple sets
   * Set
   *
   * @param key0
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply sunion(byte[][] key0) throws RedisException {
    throw notImplemented();
  }

  /**
   * Add multiple sets and store the resulting set in a key
   * Set
   *
   * @param destination0
   * @param key1
   * @return IntegerReply
   */
  @Override
  public IntegerReply sunionstore(byte[] destination0, byte[][] key1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Add one or more members to a sorted set, or update its score if it already exists
   * Sorted_set
   *
   * @param args
   * @return IntegerReply
   */
  @Override
  public IntegerReply zadd(byte[][] args) throws RedisException {
    throw notImplemented();
  }

  private double _todouble(byte[] score) {
    return parseDouble(new String(score));
  }

  /**
   * Get the number of members in a sorted set
   * Sorted_set
   *
   * @param key0
   * @return IntegerReply
   */
  @Override
  public IntegerReply zcard(byte[] key0) throws RedisException {
    ZSet zset = _getzset(key0, false);
    return integer(zset.size());
  }

  /**
   * Count the members in a sorted set with scores within the given values
   * Sorted_set
   *
   * @param key0
   * @param min1
   * @param max2
   * @return IntegerReply
   */
  @Override
  public IntegerReply zcount(byte[] key0, byte[] min1, byte[] max2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Increment the score of a member in a sorted set
   * Sorted_set
   *
   * @param key0
   * @param increment1
   * @param member2
   * @return BulkReply
   */
  @Override
  public BulkReply zincrby(byte[] key0, byte[] increment1, byte[] member2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Intersect multiple sorted sets and store the resulting sorted set in a new key
   * Sorted_set
   *
   * @param destination0
   * @param numkeys1
   * @param key2
   * @return IntegerReply
   */
  @Override
  public IntegerReply zinterstore(byte[] destination0, byte[] numkeys1, byte[][] key2) throws RedisException {
    throw notImplemented();
  }

  private IntegerReply _zstore(byte[] destination0, byte[] numkeys1, byte[][] key2, String name, boolean union) throws RedisException {
    throw notImplemented();
  }

  enum Aggregate {SUM, MIN, MAX}

  /**
   * Return a range of members in a sorted set, by index
   * Sorted_set
   *
   * @param key0
   * @param start1
   * @param stop2
   * @param withscores3
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply zrange(byte[] key0, byte[] start1, byte[] stop2, byte[] withscores3) throws RedisException {
    throw notImplemented();
  }

  private boolean _checkcommand(byte[] check, String command, boolean syntax) throws RedisException {
    boolean result;
    if (check != null) {
      if (new String(check).toLowerCase().equals(command)) {
        result = true;
      } else {
        if (syntax) {
          throw new RedisException("syntax error");
        } else {
          return false;
        }
      }
    } else {
      result = false;
    }
    return result;
  }

  /**
   * Return a range of members in a sorted set, by score
   * Sorted_set
   *
   * @param key0
   * @param min1
   * @param max2
   * @param withscores_offset_or_count4
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply zrangebyscore(byte[] key0, byte[] min1, byte[] max2, byte[][] withscores_offset_or_count4) throws RedisException {
    throw notImplemented();
  }

  private List<Reply<ByteBuf>> _zrangebyscore(byte[] min1, byte[] max2, byte[][] withscores_offset_or_count4, ZSet zset, boolean reverse) throws RedisException {
    throw notImplemented();
  }


  static class Score {
    boolean inclusive = true;
    double value;
  }

  /**
   * Determine the index of a member in a sorted set
   * Sorted_set
   *
   * @param key0
   * @param member1
   * @return Reply
   */
  @Override
  public Reply zrank(byte[] key0, byte[] member1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Remove one or more members from a sorted set
   * Sorted_set
   *
   * @param key0
   * @param member1
   * @return IntegerReply
   */
  @Override
  public IntegerReply zrem(byte[] key0, byte[][] member1) throws RedisException {
    throw notImplemented();
  }

  /**
   * Remove all members in a sorted set within the given indexes
   * Sorted_set
   *
   * @param key0
   * @param start1
   * @param stop2
   * @return IntegerReply
   */
  @Override
  public IntegerReply zremrangebyrank(byte[] key0, byte[] start1, byte[] stop2) throws RedisException {
    ZSet zset = _getzset(key0, false);
    if (zset.isEmpty()) return integer(0);
    int size = zset.size();
    int start = _torange(start1, size);
    int end = _torange(stop2, size);
    Iterator<ZSetEntry> iterator = zset.iterator();
    List<ZSetEntry> list = new ArrayList<ZSetEntry>();
    for (int i = 0; i < size; i++) {
      if (iterator.hasNext()) {
        ZSetEntry next = iterator.next();
        if (i >= start && i <= end) {
          list.add(next);
        } else if (i > end) {
          break;
        }
      }
    }
    int total = 0;
    for (ZSetEntry zSetEntry : list) {
      if (zset.remove(zSetEntry.getKey())) total++;
    }
    return integer(total);
  }

  /**
   * Remove all members in a sorted set within the given scores
   * Sorted_set
   *
   * @param key0
   * @param min1
   * @param max2
   * @return IntegerReply
   */
  @Override
  public IntegerReply zremrangebyscore(byte[] key0, byte[] min1, byte[] max2) throws RedisException {
    throw notImplemented();
  }

  /**
   * Return a range of members in a sorted set, by index, with scores ordered from high to low
   * Sorted_set
   *
   * @param key0
   * @param start1
   * @param stop2
   * @param withscores3
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply zrevrange(byte[] key0, byte[] start1, byte[] stop2, byte[] withscores3) throws RedisException {
    throw notImplemented();
  }

  /**
   * Return a range of members in a sorted set, by score, with scores ordered from high to low
   * Sorted_set
   *
   * @param key0
   * @param max1
   * @param min2
   * @param withscores_offset_or_count4
   * @return MultiBulkReply
   */
  @Override
  public MultiBulkReply zrevrangebyscore(byte[] key0, byte[] max1, byte[] min2, byte[][] withscores_offset_or_count4) throws RedisException {
    throw notImplemented();
  }

  /**
   * Determine the index of a member in a sorted set, with scores ordered from high to low
   * Sorted_set
   *
   * @param key0
   * @param member1
   * @return Reply
   */
  @Override
  public Reply zrevrank(byte[] key0, byte[] member1) throws RedisException {
    throw notImplemented();
  }


  /**
   * Get the score associated with the given member in a sorted set
   * Sorted_set
   *
   * @param key0
   * @param member1
   * @return BulkReply
   */
  @Override
  public BulkReply zscore(byte[] key0, byte[] member1) throws RedisException {
    throw notImplemented();
  }

  private byte[] _tobytes(double score) {
    return String.valueOf(score).getBytes();
  }

  /**
   * Add multiple sorted sets and store the resulting sorted set in a new key
   * Sorted_set
   *
   * @param destination0
   * @param numkeys1
   * @param key2
   * @return IntegerReply
   */
  @Override
  public IntegerReply zunionstore(byte[] destination0, byte[] numkeys1, byte[][] key2) throws RedisException {
    throw notImplemented();
  }
    public Reply srandmember(byte[] key0, byte[] count1) throws RedisException
    {
        throw new RedisException("Unimplemented");
    }

}
