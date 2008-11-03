package redpoll.text;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Tf Result Viewer, usage:
 * <p>
 * bin/hadoop jar redpoll-*.jar redpoll.text.TfResultViewer outputDir
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class TfResultViewer {

  private static Configuration conf = new Configuration();
  
  public static void main(String[] args) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(args[0] + "/tf/part-00000");
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
    Text key = new Text();
    ArrayWritable value = new ArrayWritable(TfWritable.class);
    int counter = 0;
    while ((reader.next(key, value))) {
      String str = new String(key.getBytes(), 0, key.getLength());
      System.out.print(str + "\t");
      Writable[] tfs = value.get();
      for(int i = 0; i < tfs.length; i ++) {
        System.out.print(tfs[i].toString() + " ");
      }
      System.out.println();
      counter ++;
    }
    System.out.println("result count:\t" + counter);
    reader.close();
  }
}
