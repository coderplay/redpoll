/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package redpoll.clusterer.canopy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import redpoll.core.WritableSparseVector;
import redpoll.core.WritableVector;

public class ClusterMapper extends MapReduceBase implements
        Mapper<Text, WritableVector, Text, WritableVector> {

  private List<Canopy> canopies;

  public void map(Text key, WritableVector value,
                  OutputCollector<Text, WritableVector> output, Reporter reporter) throws IOException {
    Canopy.emitPointToExistingCanopies(value, canopies, value, output);
  }

  /**
   * Configure the mapper by providing its canopies. Used by unit tests.
   *
   * @param canopies a List<Canopy>
   */
  public void config(List<Canopy> canopies) {
    this.canopies = canopies;
  }

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    Canopy.configure(job);

    String canopyPath = job.get(Canopy.CANOPY_PATH_KEY);
    canopies = new ArrayList<Canopy>();

    try {
      FileSystem fs = FileSystem.get(job);
      Path path = new Path(canopyPath + "/part-00000");
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, job);
      try {
        Text key = new Text();
        WritableSparseVector value = new WritableSparseVector();
        while (reader.next(key, value)) {
          int canopyId = Integer.parseInt(key.toString().substring(1));
          Canopy canopy = new Canopy(value, canopyId);
          canopies.add(canopy);
        }
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
