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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import redpoll.text.Document;

/**
 * Treats keys as offset in file and value as an document. 
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class SogouRecordReader implements RecordReader<LongWritable, Document>{

  private static final Log LOG 
    = LogFactory.getLog(SogouRecordReader.class.getName());
  
  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long end;
  private long pos;
  
  private SogouCorpusReader in;
    
  public SogouRecordReader(Configuration job, 
      FileSplit split) throws IOException {  
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    if (codec != null) {
      in = new SogouCorpusReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) 
        fileIn.seek(start);
      in = new SogouCorpusReader(fileIn, job);
    }
    this.pos = start;
  }
  
  public LongWritable createKey() {
    return new LongWritable();
  }

  public Document createValue() {
    return new SogouDocument();
  }

  
  public long getPos() throws IOException {
    return pos;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  /**
   * Close the input stream
   */
  public void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }

  public synchronized boolean next(LongWritable key, Document value)
      throws IOException {
    if(pos < end) {
      long docPos = in.nextDoc((SogouDocument)value);
      if(docPos < 0)
        return false;
      key.set(start + docPos);
      pos = start + in.getPosition();
      return true;
    }
    return false;
  }


}
