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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Class to operate hbase document table.
 * 
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class HBaseDocumentOperator {
  
  private static final Log log = LogFactory.getLog(HBaseDocumentOperator.class
      .getName());
  
  static final String CORPUS_TABLE_NAME = "corpus";
  static final String PATH_COLUMN = "path:";
  static final String TITLE_COLUMN = "title:";
  static final String CONTENT_COLUMN = "content:";

  private static final byte[] tableName = Bytes
      .toBytes(CORPUS_TABLE_NAME);
  private static final byte[][] columns = new byte[][] {
      Bytes.toBytes(PATH_COLUMN), Bytes.toBytes(TITLE_COLUMN),
      Bytes.toBytes(CONTENT_COLUMN) };
  
  private static HBaseDocumentOperator instance = null;
  
  
  /* hbase table stores the documents */
  private HTable table;
 
  HBaseDocumentOperator() {
    HBaseConfiguration conf = new HBaseConfiguration();
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      HTableDescriptor tableDesc =
        new HTableDescriptor(tableName);
      tableDesc.addFamily(new HColumnDescriptor(columns[0]));
      tableDesc.addFamily(new HColumnDescriptor(columns[1]));
      tableDesc.addFamily(new HColumnDescriptor(columns[2]));
      admin.createTable(tableDesc);
      table = new HTable(conf, tableName);
    }
    catch(Exception exc) {
      log.error(exc.getMessage());
    }
  }
  
  /**
   * singleton pattern.
   */
  public static HBaseDocumentOperator getInstance() {
    if(instance == null) {
      instance = new HBaseDocumentOperator();
    }
    return instance;
  }
  
  /**
   * Insert a document into the hbase table.
   * @param doc document which will be inserted into the hbase table.
   * @throws IOException occurs when inserting operation failed.
   */
  public void insert(Document doc) throws IOException {
    BatchUpdate batchUpdate = new BatchUpdate(doc.getDocuemntId());
    batchUpdate.put(columns[0], Bytes.toBytes(doc.getPath()));
    batchUpdate.put(columns[1], Bytes.toBytes(doc.getTitle()));
    batchUpdate.put(columns[2], Bytes.toBytes(doc.getContent()));
    log.info("updating document: " + doc.getDocuemntId());
    table.commit(batchUpdate);
  }
  
  /**
   * Get a document from the database with a specific document id
   * @param id document id
   * @return the document selecting by its document id
   * @throws IOException occurs when selecting operation failed.
   */
  public Document select(String id) throws IOException{
    //TODO: implement it!
    return null;
  }
}
