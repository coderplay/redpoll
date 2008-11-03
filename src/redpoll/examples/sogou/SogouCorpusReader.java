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

package redpoll.examples.sogou;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;

/**
 * A class that provides a sogou document reader from an input stream.
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class SogouCorpusReader {

  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  
  /* input stream which we will get documents from */
  private InputStream in;
  /* a buffer stores temporary bytes readed from input stream */
  private byte[] buffer;
  /* the number of bytes of real data in the buffer */
  private int bufferLength = 0;
  /* the current position of the buffer */
  private int posAtBuffer = 0;
  /* the buffer position of input stream */
  private long bufferPos = 0;
  /* current document position of input stream */
  private long currentDocPosn = 0;

  /* xml-like mark tags used in sogou corpus */
  private byte[] docTag;
  private byte[] urlTag;
  private byte[] docnoTag;
  private byte[] titleTag;
  private byte[] contentTag;

  /* parser status */
  static enum STATUS {
    PREPARE, START_ELEMENT, END_ELEMENT, TEXT
  };

  /* used for defining current parsing node */
  static enum NODE {
    NULL, DOC, URL, DOC_NO, TITLE, CONTENT, FAILED, SUCCEED
  };

  private STATUS currentSatus;
  private NODE currentNode;

  public SogouCorpusReader(InputStream in) throws IOException {
    this(in, DEFAULT_BUFFER_SIZE);
  }

  public SogouCorpusReader(InputStream in, int bufferSize) throws IOException {
    this(in, bufferSize, "doc", "url", "docno", "contenttitle", "content");
  }

  public SogouCorpusReader(InputStream in, int bufferSize, String doc,
      String url, String docno, String title, String content)
      throws IOException {
    this.in = in;
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufferSize];
    docTag = doc.getBytes("UTF-8");
    urlTag = url.getBytes("UTF-8");
    docnoTag = docno.getBytes("UTF-8");
    titleTag = title.getBytes("UTF-8");
    contentTag = content.getBytes("UTF-8");
  }

  public SogouCorpusReader(InputStream in, Configuration conf)
      throws IOException {
    this(in, conf.getInt("redpoll.sogou.doc.buffersize", DEFAULT_BUFFER_SIZE),
    conf.get("redpoll.sogou.doc", "doc"), 
    conf.get("redpoll.sogou.doc.url","url"), 
    conf.get("redpoll.sogou.doc.docno", "docno"), 
    conf.get("redpoll.sogou.doc.contenttitle", "contenttitle"), 
    conf.get("redpoll.sogou.doc.content", "content"));
  }

  /**
   * Gets a {@link redpoll.text.Document} instance from sogou text file. If it reached EOF, it willreturn the end position of inputstream. 
   * @param  a {@link redpoll.text.Document} instance getting from sogou text file. 
   * @return the end position of this document, or the end position of inputstream if it reached EOF. 
   * @throws IOException
   */
  public long nextDoc(SogouDocument doc) throws IOException {
    currentSatus = STATUS.PREPARE;
    currentNode = NODE.NULL;
    if(bufferLength < 0) return -1; // has reached EOF
    try {
      while (true) {
        adjustBuffer();
        if (currentSatus == STATUS.PREPARE) {
          if (buffer[posAtBuffer] == '<')
            currentSatus = STATUS.START_ELEMENT;
        } else if (currentSatus == STATUS.START_ELEMENT) {
          if (buffer[posAtBuffer] == '/') { // e.g. </node>
            currentSatus = STATUS.END_ELEMENT;
          } else {
            int start = posAtBuffer; byte[] name = null;
            while (buffer[posAtBuffer] != '>' && buffer[posAtBuffer] != '\n') {
              posAtBuffer++;
              if(posAtBuffer >= bufferLength) {
                name = new byte[bufferLength - start];
                System.arraycopy(buffer, start, name, 0, bufferLength - start);
                start = 0;
              }
              adjustBuffer();
            }
            // if a element ends with '\n', we consider it as a wrong element
            if (buffer[posAtBuffer] == '\n') 
              failed(); // FAILED
            else if (buffer[posAtBuffer] == '>') {
              int len = posAtBuffer - start;
              if (len > 0) {
                if (name != null) {
                  byte[] newname = new byte[name.length + len];
                  System.arraycopy(name, 0, newname, 0, name.length);
                  System.arraycopy(buffer, start, newname, name.length, len);
                  name = newname;
                } else {
                  name = new byte[len];
                  System.arraycopy(buffer, start, name, 0, len);
                }
                startElement(name);
              }
              ignoreWhite();
              currentSatus = STATUS.TEXT;
            }
          }
        } else if (currentSatus == STATUS.TEXT) {
          int start = posAtBuffer; byte[] text = null;
          while (buffer[posAtBuffer] != '<' && buffer[posAtBuffer] != '\n') {
            posAtBuffer++;
            if(posAtBuffer >= bufferLength) {
              // FIXME: if the content of a document passes through more than two buffers, it will get wrong! 
              text = new byte[bufferLength - start];
              System.arraycopy(buffer, start, text, 0, bufferLength - start);
              start = 0;
            }
            adjustBuffer();
          }
          if (buffer[posAtBuffer] == '<') {
            int len = posAtBuffer - start;
            if (len > 0) {
              if (text != null) {
                byte[] newtext = new byte[text.length + len];
                System.arraycopy(text, 0, newtext, 0, text.length);
                System.arraycopy(buffer, start, newtext, text.length, len);
                text = newtext;
              } else {
                text = new byte[len];
                System.arraycopy(buffer, start, text, 0, len);
              }
              characters(text, doc);
            }
            currentSatus = STATUS.START_ELEMENT;
          } else if (buffer[posAtBuffer] == '\n')
            failed(); // FAILED
        } else if (currentSatus == STATUS.END_ELEMENT) {
          int start = posAtBuffer; byte[] name = null;
          while (buffer[posAtBuffer] != '>' && buffer[posAtBuffer] != '\n') {
            posAtBuffer++;
            if(posAtBuffer >= bufferLength) {
              name = new byte[bufferLength - start];
              System.arraycopy(buffer, start, name, 0, bufferLength - start);
              start = 0;
            }
            adjustBuffer();
          }
          if (buffer[posAtBuffer] == '>') {
            int len = posAtBuffer - start;
            if (len > 0) {
              if (name != null) {
                byte[] newname = new byte[name.length + len];
                System.arraycopy(name, 0, newname, 0, name.length);
                System.arraycopy(buffer, start, newname, name.length, len);
                name = newname;
              } else {
                name = new byte[len];
                System.arraycopy(buffer, start, name, 0, len);
              }
              endElement(name);
              if(currentNode == NODE.SUCCEED) break;
            }
            ignoreWhite();
            currentSatus = STATUS.PREPARE;
          } else if (buffer[posAtBuffer] != '\n')
            failed(); // FAILED
        }
        posAtBuffer++;
      }
    } catch (EOFException eofe) {
      return -1;
    }
    return currentDocPosn;
  }
  
  public long getPosition() {
    return bufferPos + posAtBuffer;
  }

  /**
   * Close the underlying stream.
   * @throws IOException
   */
  public void close() throws IOException {
    in.close();
  }
  
  private void ignoreWhite() throws IOException, EOFException {
    do {
      posAtBuffer++;
      adjustBuffer();
    } while (buffer[posAtBuffer] == '\n' || buffer[posAtBuffer] == '\r'
      || buffer[posAtBuffer] == '\t' || buffer[posAtBuffer] == ' ');
    posAtBuffer--;
  }

  private void adjustBuffer() throws IOException, EOFException {
    if (posAtBuffer >= bufferLength) {
      bufferPos += bufferLength;
      posAtBuffer = 0;
      bufferLength = in.read(buffer);
      if (bufferLength <= 0) {
        posAtBuffer = -1;
        throw new EOFException();
      }
    }
  }
  
  
  private void startElement(byte[] name) {
    if ((currentNode == NODE.NULL || currentNode == NODE.FAILED) && equals(docTag, name)) {
      currentDocPosn = bufferPos + posAtBuffer - docTag.length - 1;
      currentNode = NODE.DOC;
    } else if (currentNode == NODE.DOC && equals(urlTag, name)) {
      currentNode = NODE.URL;
    } else if (currentNode == NODE.URL && equals(docnoTag, name)) {
      currentNode = NODE.DOC_NO;
    } else if (currentNode == NODE.DOC_NO && equals(titleTag, name)) {
      currentNode = NODE.TITLE;
    } else if (currentNode == NODE.TITLE && equals(contentTag, name)) {
      currentNode = NODE.CONTENT;
    } else {
      currentNode = NODE.FAILED;
    }
  }

  private void endElement(byte[] name) {
    if (currentNode == NODE.CONTENT && equals(docTag, name)) {
      currentNode = NODE.SUCCEED;
    }
  }
  
  private void characters(byte[] text, SogouDocument doc) {
    if (currentNode == NODE.URL) {
      doc.setPathBytes(text);
    } else if (currentNode == NODE.DOC_NO) {
      doc.setIdBytes(text);
    } else if (currentNode == NODE.TITLE) {
      doc.setTitleBytes(text);
    } else if (currentNode == NODE.CONTENT) {
      doc.setContentBytes(text);
    }
  }

  private void failed() {
    currentNode = NODE.FAILED;
  }
  
  private boolean equals(final byte [] left, final byte [] right) {
    return left == null && right == null? true:
      left == null && right != null? false:
      left != null && right == null? false:
      left.length != right.length? false:
        WritableComparator.compareBytes(left, 0, left.length, right, 0, right.length) == 0;
  }
  
  /**
   *  for test
   */
  public static void main(String[] args) throws IOException {
    FileInputStream in = new FileInputStream(args[0]);
    //in.skip(10);
    SogouCorpusReader reader = new SogouCorpusReader(in);
    SogouDocument doc = new SogouDocument();
    int counter = 0; long pos = 0;
    long start = System.currentTimeMillis();
    while ((pos = reader.nextDoc(doc)) >= 0 ) {
      System.out.println("docId:" + pos + "\tpos:" + reader.getPosition());
    }
//    System.out.println(System.currentTimeMillis() - start );
//    System.out.println(counter);
  }

}
