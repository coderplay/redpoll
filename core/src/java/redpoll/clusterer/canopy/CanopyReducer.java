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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import redpoll.core.LabeledWritableVector;
import redpoll.core.WritableVector;

public class CanopyReducer extends MapReduceBase implements
        Reducer<Text, WritableVector, Text, WritableVector> {

  private static final Log log = LogFactory.getLog(CanopyReducer.class.getName());
  
  private final List<Canopy> canopies = new ArrayList<Canopy>();

  public void reduce(Text key, Iterator<WritableVector> values,
                     OutputCollector<Text, WritableVector> output, Reporter reporter) throws IOException {
    while (values.hasNext()) {
      LabeledWritableVector point = (LabeledWritableVector) values.next();
      Canopy.emitPointToNewCanopies(point, canopies, output);
    }
  }

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    Canopy.configure(job);
  }

}
