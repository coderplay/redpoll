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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Reads sogou corpus files into a hbase table.
 * 
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class SogouCorpusPreparer {
  
  public static void main(String[] args) throws UnsupportedEncodingException,
      FileNotFoundException, IOException {
    File inputDir = new File(args[0]);
    File[] files = inputDir.listFiles();
    Document doc = null;
    HBaseDocumentOperator filler = HBaseDocumentOperator.getInstance();
    for (File file : files) {
      SogouDocumentParser preparer = new SogouDocumentParser(file);
      while ((doc = preparer.getNextDoc()) != null) {
        filler.insert(doc);
      }
    }
  }
}