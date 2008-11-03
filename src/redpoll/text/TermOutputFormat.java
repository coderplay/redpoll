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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This class allows writing the output data to different output files in
 * sequence file output format.
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class TermOutputFormat extends FileOutputFormat<Text, TermWritable> {
  
  protected static class TermWriter implements
      RecordWriter<Text, TermWritable> {
    
    private final String myName;
    private final JobConf myJob;
    private final Progressable myProgress;
    
    private RecordWriter<Text, TfArrayWritable> tfWriter;
    private RecordWriter<Text, IntWritable> dfWriter;
        
    public TermWriter(JobConf job, String name, Progressable progress)
        throws IOException {
      myName = name;
      myJob = job;
      myProgress = progress;
    }

    public void close(Reporter reporter) throws IOException {
      tfWriter.close(reporter);
      dfWriter.close(reporter);
    }

    public void write(Text key, TermWritable value)
        throws IOException {
      Writable val = value.get();
      boolean isTf = (val instanceof TfArrayWritable);
      String path = isTf ? "tf/" + myName : "df/" + myName;
      // get the file name based on the input file name
      String finalPath = getInputFileBasedOutputFileName(myJob, path);

      if(isTf) {
        if(tfWriter == null) {
          tfWriter = getTfRecordWriter(myJob, finalPath, myProgress);
        }
        tfWriter.write(key, (TfArrayWritable) val);
      }
      else {
        if(dfWriter == null) {
          dfWriter = getDfRecordWriter(myJob, finalPath, myProgress);
        }
        dfWriter.write(key, (IntWritable) val);
      }
    }
  }
  
  protected static RecordWriter<Text, TfArrayWritable> getTfRecordWriter(
      JobConf job, String name, Progressable progress) throws IOException {
    Path file = FileOutputFormat.getTaskOutputPath(job, name);
    FileSystem fs = file.getFileSystem(job);

    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    if (getCompressOutput(job)) {
      // find the kind of compression to do
      compressionType = getOutputCompressionType(job);
      // find the right codec
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
          job, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, job);
    }

    final SequenceFile.Writer out = SequenceFile.createWriter(fs, job, file,
        Text.class, TfArrayWritable.class, compressionType, codec, progress);

    return new RecordWriter<Text, TfArrayWritable>() {
      public void write(Text key, TfArrayWritable value) throws IOException {
        out.append(key, value);
      }

      public void close(Reporter reporter) throws IOException {
        out.close();
      }
    };
  }
 
  protected static RecordWriter<Text, IntWritable> getDfRecordWriter(
      JobConf job, String name, Progressable progress) throws IOException {
    Path file = FileOutputFormat.getTaskOutputPath(job, name);
    FileSystem fs = file.getFileSystem(job);

    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    if (getCompressOutput(job)) {
      // find the kind of compression to do
      compressionType = getOutputCompressionType(job);
      // find the right codec
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
          job, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, job);
    }

    final SequenceFile.Writer out = SequenceFile.createWriter(fs, job, file,
        Text.class, IntWritable.class, compressionType, codec, progress);

    return new RecordWriter<Text, IntWritable>() {
      public void write(Text key, IntWritable value) throws IOException {
        out.append(key, value);
      }

      public void close(Reporter reporter) throws IOException {
        out.close();
      }
    };
  }
  
  /**
   * Generate the outfile name based on a given anme and the input file name. If
   * the map input file does not exists (i.e. this is not for a map only job),
   * the given name is returned unchanged. If the config value for
   * "num.of.trailing.legs.to.use" is not set, or set 0 or negative, the given
   * name is returned unchanged. Otherwise, return a file name consisting of the
   * N trailing legs of the input file name where N is the config value for
   * "num.of.trailing.legs.to.use".
   * 
   * @param job
   *          the job config
   * @param name
   *          the output file name
   * @return the outfile name based on a given anme and the input file name.
   */
  protected static String getInputFileBasedOutputFileName(JobConf job, String name) {
    String infilepath = job.get("map.input.file");
    if (infilepath == null) {
      // if the map input file does not exists, then return the given name
      return name;
    }
    int numOfTrailingLegsToUse = job.getInt("mapred.outputformat.numOfTrailingLegs", 0);
    if (numOfTrailingLegsToUse <= 0) {
      return name;
    }
    Path infile = new Path(infilepath);
    Path parent = infile.getParent();
    String midName = infile.getName();
    Path outPath = new Path(midName);
    for (int i = 1; i < numOfTrailingLegsToUse; i++) {
      if (parent == null) break;
      midName = parent.getName();
      if (midName.length() == 0) break;
      parent = parent.getParent();
      outPath = new Path(midName, outPath);
    }
    return outPath.toString();
  }
  
  /**
   * Get the {@link CompressionType} for the output {@link SequenceFile}.
   * @param conf the {@link JobConf}
   * @return the {@link CompressionType} for the output {@link SequenceFile}, 
   *         defaulting to {@link CompressionType#RECORD}
   */
  public static CompressionType getOutputCompressionType(JobConf conf) {
    String val = conf.get("mapred.output.compression.type", 
                          CompressionType.RECORD.toString());
    return CompressionType.valueOf(val);
  }
  
  @Override
  public RecordWriter<Text, TermWritable> getRecordWriter(FileSystem fs,
      JobConf job, String name, Progressable progress) throws IOException {
    return new TermWriter(job, name, progress);
  }

}
