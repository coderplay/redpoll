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

package redpoll.examples;


/**
 * Document which will be stored or mined.
 * 
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class Document {
  
  private String documentId;
  private String path;
  private String title;
  private String content;
  
  public Document(String id, String path, String title, String content) {
    this.documentId = id;
    this.path = path;
    this.title = title;
    this.content = content;
  }
  
  public String getDocuemntId() {
    return documentId;
  }
  
  /**
   * path or url.
   */
  public String getPath() {
    return title;
  }
  
  public String getTitle() {
    return title;
  }
  
  public String getContent() {
    return content;
  }
  
  public String toString() {
    return documentId + " " + path + " " + title + " " + content; 
  }
}
