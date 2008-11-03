/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package redpoll.text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class RedpollStringWritable extends BinaryComparable
  implements WritableComparable<BinaryComparable>{

  private static Charset charset;
  private static CharsetDecoder decoder;
  private static CharsetEncoder encoder;
  
  static {
    setCharset(Charset.forName("UTF-8"));
  }
  
  public static void setCharset(Charset cs) {
    charset = cs;
    decoder = cs.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);
    encoder = cs.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);
  }
    
  private static final byte [] EMPTY_BYTES = new byte[0];
  
  private byte[] bytes;
  private int length;

  public RedpollStringWritable() {
    bytes = EMPTY_BYTES;
  }

  /** Construct from a string. 
   */
  public RedpollStringWritable(String string) {
    set(string);
  }

  /** Construct from another text. */
  public RedpollStringWritable(RedpollStringWritable rs) {
    set(rs);
  }

  /** Construct from a byte array.
   */
  public RedpollStringWritable(byte[] bytes)  {
    set(bytes);
  }
  
  /**
   * Returns the raw bytes; however, only data up to {@link #getLength()} is
   * valid.
   */
  public byte[] getBytes() {
    return bytes;
  }

  /** Returns the number of bytes in the byte array */ 
  public int getLength() {
    return length;
  }
    
  public int find(String what) {
    return find(what, 0);
  }
  
  /**
   * Finds any occurence of <code>what</code> in the backing
   * buffer, starting as position <code>start</code>. The starting
   * position is measured in bytes and the return value is in
   * terms of byte position in the buffer. The backing buffer is
   * not converted to a string for this operation.
   * @return byte position of the first occurence of the search
   *         string in the UTF-8 buffer or -1 if not found
   */
  public int find(String what, int start) {
    try {
      ByteBuffer src = ByteBuffer.wrap(this.bytes,0,this.length);
      ByteBuffer tgt = encode(what);
      byte b = tgt.get();
      src.position(start);
          
      while (src.hasRemaining()) {
        if (b == src.get()) { // matching first byte
          src.mark(); // save position in loop
          tgt.mark(); // save position in target
          boolean found = true;
          int pos = src.position()-1;
          while (tgt.hasRemaining()) {
            if (!src.hasRemaining()) { // src expired first
              tgt.reset();
              src.reset();
              found = false;
              break;
            }
            if (!(tgt.get() == src.get())) {
              tgt.reset();
              src.reset();
              found = false;
              break; // no match
            }
          }
          if (found) return pos;
        }
      }
      return -1; // not found
    } catch (CharacterCodingException e) {
      // can't get here
      e.printStackTrace();
      return -1;
    }
  }  
  /** Set to contain the contents of a string. 
   */
  public void set(String string) {
    try {
      ByteBuffer bb = encode(string, true);
      bytes = bb.array();
      length = bb.limit();
    }catch(CharacterCodingException e) {
      throw new RuntimeException("Should not have happened " + e.toString()); 
    }
  }

  /** Set to a utf8 byte array
   */
  public void set(byte[] bytes) {
    set(bytes, 0, bytes.length);
  }
  
  /** copy a text. */
  public void set(RedpollStringWritable other) {
    set(other.getBytes(), 0, other.getLength());
  }

  /**
   * Set the Text to range of bytes
   * @param source the data to copy from
   * @param start the first position of the new string
   * @param len the number of bytes of the new string
   */
  public void set(byte[] source, int start, int len) {
    setCapacity(len, false);
    System.arraycopy(source, start, bytes, 0, len);
    this.length = len;
  }

  /**
   * Append a range of bytes to the end of the given text
   * @param source the data to copy from
   * @param start the first position to append from utf8
   * @param len the number of bytes to append
   */
  public void append(byte[] source, int start, int len) {
    setCapacity(length + len, true);
    System.arraycopy(source, start, bytes, length, len);
    length += len;
  }

  /**
   * Clear the string to empty.
   */
  public void clear() {
    length = 0;
  }

  /*
   * Sets the capacity of this Text object to <em>at least</em>
   * <code>len</code> bytes. If the current buffer is longer,
   * then the capacity and existing content of the buffer are
   * unchanged. If <code>len</code> is larger
   * than the current capacity, the Text object's capacity is
   * increased to match.
   * @param len the number of bytes we need
   * @param keepData should the old data be kept
   */
  private void setCapacity(int len, boolean keepData) {
    if (bytes == null || bytes.length < len) {
      byte[] newBytes = new byte[len];
      if (bytes != null && keepData) {
        System.arraycopy(bytes, 0, newBytes, 0, length);
      }
      bytes = newBytes;
    }
  }
   
  /** 
   * Convert text back to string
   * @see java.lang.Object#toString()
   */
  public String toString() {
    try {
      return decode(bytes, 0, length);
    } catch (CharacterCodingException e) { 
      throw new RuntimeException("Should not have happened " + e.toString()); 
    }
  }
  
  /** deserialize 
   */
  public void readFields(DataInput in) throws IOException {
    int newLength = WritableUtils.readVInt(in);
    setCapacity(newLength, false);
    in.readFully(bytes, 0, newLength);
    length = newLength;
  }

  /** Skips over one Text in the input. */
  public static void skip(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    WritableUtils.skipFully(in, length);
  }

  /** serialize
   * write this object to out
   * length uses zero-compressed encoding
   * @see Writable#write(DataOutput)
   */
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, length);
    out.write(bytes, 0, length);
  }

  /** Returns true iff <code>o</code> is a Text with the same contents.  */
  public boolean equals(Object o) {
    if (o instanceof RedpollStringWritable)
      return super.equals(o);
    return false;
  }

  public int hashCode() {
    return super.hashCode();
  }

  /// STATIC UTILITIES FROM HERE DOWN
  /**
   * Converts the provided byte array to a String using the
   * UTF-8 encoding. If the input is malformed,
   * replace by a default value.
   */
  public static String decode(byte[] bytes) throws CharacterCodingException {
    return decode(ByteBuffer.wrap(bytes), true);
  }
  
  public static String decode(byte[] bytes, int start, int length) 
    throws CharacterCodingException {
    return decode(ByteBuffer.wrap(bytes, start, length), true);
  }
  
  /**
   * Converts the provided byte array to a String using the
   * UTF-8 encoding. If <code>replace</code> is true, then
   * malformed input is replaced with the
   * substitution character, which is U+FFFD. Otherwise the
   * method throws a MalformedInputException.
   */
  public static String decode(byte[] bytes, int start, int length, boolean replace) 
    throws CharacterCodingException {
    return decode(ByteBuffer.wrap(bytes, start, length), replace);
  }
  
  private static String decode(ByteBuffer byteBuffer, boolean replace) 
    throws CharacterCodingException {
    if (replace) {
      decoder.onMalformedInput(
          java.nio.charset.CodingErrorAction.REPLACE);
      decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
    String str = decoder.decode(byteBuffer).toString();
    // set decoder back to its default value: REPORT
    if (replace) {
      decoder.onMalformedInput(CodingErrorAction.REPORT);
      decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    }
    return str;
  }

  /**
   * Converts the provided String to bytes using the
   * UTF-8 encoding. If the input is malformed,
   * invalid chars are replaced by a default value.
   * @return ByteBuffer: bytes stores at ByteBuffer.array() 
   *                     and length is ByteBuffer.limit()
   */

  public static ByteBuffer encode(String string)
    throws CharacterCodingException {
    return encode(string, true);
  }

  /**
   * Converts the provided String to bytes using the
   * UTF-8 encoding. If <code>replace</code> is true, then
   * malformed input is replaced with the
   * substitution character, which is U+FFFD. Otherwise the
   * method throws a MalformedInputException.
   * @return ByteBuffer: bytes stores at ByteBuffer.array() 
   *                     and length is ByteBuffer.limit()
   */
  public static ByteBuffer encode(String string, boolean replace)
    throws CharacterCodingException {
    if (replace) {
      encoder.onMalformedInput(CodingErrorAction.REPLACE);
      encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
    ByteBuffer bytes = 
      encoder.encode(CharBuffer.wrap(string.toCharArray()));
    if (replace) {
      encoder.onMalformedInput(CodingErrorAction.REPORT);
      encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    }
    return bytes;
  }

  
  /** A WritableComparator optimized for Text keys. */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(RedpollStringWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      int n1 = WritableUtils.decodeVIntSize(b1[s1]);
      int n2 = WritableUtils.decodeVIntSize(b2[s2]);
      return compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
    }
  }

  static {
    // register this comparator
    WritableComparator.define(RedpollStringWritable.class, new Comparator());
  }
}
