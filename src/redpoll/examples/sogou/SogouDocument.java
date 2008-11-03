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

import java.nio.charset.Charset;

import redpoll.text.Document;

/**
 * Sogou document implementation.
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class SogouDocument implements Document {
  
  private static final Charset DEFAULT_CHARSET = Charset.forName("GBK");
  private static final byte [] EMPTY_BYTES = new byte[0];
  
  private Charset charset;
  
  private byte[] documentIdBytes;
  private byte[] pathBytes;
  private byte[] titleBytes;
  private byte[] contentBytes;
  
  private String documentId  = null;
  private String path = null;
  private String title = null;
  private String content = null;
  
  public SogouDocument() {
    this(DEFAULT_CHARSET);
  }
  
  public SogouDocument(String encoding) {
    this(Charset.forName(encoding));
  }
  
  public SogouDocument(Charset charset) {
    this.charset = charset;
    documentIdBytes = EMPTY_BYTES;
    pathBytes = EMPTY_BYTES;
    titleBytes = EMPTY_BYTES;
    contentBytes = EMPTY_BYTES;
  }
  
  public void setIdBytes(byte[] idBytes) {
    this.documentIdBytes = idBytes;
    documentId = null;
  }

  public void setPathBytes(byte[] pathBytes) {
    this.pathBytes = pathBytes;
    path = null;
  }

  public void setTitleBytes(byte[] titleBytes) {
    this.titleBytes = titleBytes;
    title = null;
  }

  public void setContentBytes(byte[] contentBytes) {
    this.contentBytes = contentBytes;
    content = null;
  }
  
  /** url */
  public String getPath() {
    if(path == null)
      path = new String(pathBytes, charset);
    return path;
  }
  
  /** contenttitle */
  public String getTitle() {
    if(title == null)
      title = new String(titleBytes, charset);
    return title;
  }
  
  /** content */
  public String getContent() {
    if(content == null)
      content = new String(contentBytes, charset);
    return content;
  }
  
  /** docno */
  public String getDocumentId() {
    if(documentId == null)
      documentId = new String(documentIdBytes, charset);
    return documentId;
  }  
}
