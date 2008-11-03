/**
 *  Licensed to the Apache Software Foundation (ASF) under one
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redpoll.text.Document;
import redpoll.text.SimpleDocument;

/**
 * It Reads sogou corpus from text files with special format into a hbase table.
 * 
 * @author muggsky (muggsky@gmail.com)
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class SogouDocumentParser {

  private static final Log log = LogFactory.getLog(SogouDocumentParser.class
      .getName());

  /* counter for generate an unique document id */
  private static int counter = 0;

  /* patterns for match documents */
  private Pattern[] patterns = { Pattern.compile("<url>(.*)</url>"),
      Pattern.compile("<docno>(.*)</docno>"),
      Pattern.compile("<contenttitle>(.*)</contenttitle>"),
      Pattern.compile("<content>(.*)</content>") };

  /* input file reader */
  private BufferedReader reader;

  public SogouDocumentParser(File file) throws UnsupportedEncodingException,
      IOException {
    reader = new BufferedReader(new InputStreamReader(
        new FileInputStream(file), "GBK"));
  }

  /**
   * Scan the input file and return a document with an unique id from it.
   * @return document getting from corpus file.
   */
  public Document getNextDoc() {
    String line = null;
    try {
      while ((line = reader.readLine()) != null) {
        String[] doc = new String[4];
        if (line.charAt(0) == '<' && ((doc[0] = reader.readLine()) != null)
            && ((doc[1] = reader.readLine()) != null)
            && ((doc[2] = reader.readLine()) != null)
            && ((doc[3] = reader.readLine()) != null)) {
          Matcher[] m = { patterns[0].matcher(doc[0]),
              patterns[1].matcher(doc[1]), patterns[2].matcher(doc[2]),
              patterns[3].matcher(doc[3]) };
          if (m[0].matches() && m[1].matches() && m[2].matches()
              && m[3].matches()) {
            Document result = new SimpleDocument("" + counter, m[0].group(1), m[2].group(1), m[3].group(1));
            counter++;
            reader.readLine();
            return result;
          }
        }
      }
      reader.close();
    } catch (IOException excp) {
      log.error("Error occurs while parsing sogou corpus" + excp.getMessage());
    }
    return null;
  }
}
