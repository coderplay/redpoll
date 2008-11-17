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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import redpoll.core.LabeledWritableVector;
import redpoll.core.Vector;
import redpoll.core.WritableSparseVector;
import redpoll.core.WritableVector;
import redpoll.util.DistanceMeasure;

/**
 * This class models a canopy as a center point.
 */
public class Canopy {

  // keys used by Driver, Mapper & Reducer
  public static final String DISTANCE_MEASURE_KEY = "redpoll.clusterer.canopy.measure";

  public static final String T1_KEY = "redpoll.clusterer.canopy.t1";

  public static final String T2_KEY = "redpoll.clusterer.canopy.t2";

  public static final String CANOPY_PATH_KEY = "redpoll.clusterer.canopy.path";

  private static final Log log = LogFactory.getLog(Canopy.class.getName());


  // the T1 distance threshold
  private static double t1;

  // the T2 distance threshold
  private static double t2;

  // the distance measure
  private static DistanceMeasure measure;

  // this canopy's canopyId
  private final Text canopyId;

  // the current center
  private WritableVector center = new WritableSparseVector(0);

  public Canopy(String idString, WritableVector point) {
    super();
    this.canopyId = new Text(idString);
    this.center = point;
  }
  

  /**
   * Create a new Canopy containing the given labeled point.
   * @param point a labeled point in vector space
   */
  public Canopy(LabeledWritableVector point) {
    super();
    this.canopyId = point.getLabel();
    this.center = point;
  }
  

  /**
   * Configure the Canopy and its distance measure
   * @param job the JobConf for this job
   */
  public static void configure(JobConf job) {
    try {
      final ClassLoader ccl = Thread.currentThread().getContextClassLoader();
      Class<?> cl = ccl.loadClass(job.get(DISTANCE_MEASURE_KEY));
      measure = (DistanceMeasure) cl.newInstance();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    }
    t1 = Double.parseDouble(job.get(T1_KEY));
    t2 = Double.parseDouble(job.get(T2_KEY));
  }

  /**
   * Configure the Canopy for unit tests
   * @param aMeasure
   * @param aT1
   * @param aT2
   */
  public static void config(DistanceMeasure aMeasure, double aT1, double aT2) {
    measure = aMeasure;
    t1 = aT1;
    t2 = aT2;
  }


  /**
   * This method is used by the CanopyMapper to perform canopy inclusion tests
   * and to emit the point and its covering canopies to the output.
   * 
   * @param point the point to be added
   * @param canopies the List<Canopy> to be appended
   * @param collector an OutputCollector in which to emit the point
   */
  public static void emitPointToNewCanopies(LabeledWritableVector point,
      List<Canopy> canopies, OutputCollector<Text, WritableVector> collector)
      throws IOException {
    boolean pointStronglyBound = false;
    for (Canopy canopy : canopies) {
      double dist = measure.distance(canopy.getCenter(), point);
      pointStronglyBound = pointStronglyBound || (dist < t2);
    }
    if (!pointStronglyBound) {
      // strong bound
      Canopy newCanopy = new Canopy(point.copy());
      canopies.add(newCanopy);
      collector.collect(new Text("canopy"), newCanopy.getCenter());
    }
  }

  /**
   * This method is used by the ClusterMapper to perform canopy inclusion tests
   * and to emit the point keyed by its covering canopies to the output. if the
   * point is not covered by any canopies (due to canopy centroid clustering),
   * emit the point to the closest covering canopy.
   * 
   * @param point the point to be added
   * @param canopies the List<Canopy> to be appended
   * @param writable the original Writable from the input, may include arbitrary
   *                payload information after the point [...]<payload>
   * @param collector an OutputCollector in which to emit the point
   */
  public static void emitPointToExistingCanopies(Text key,
      List<Canopy> canopies, WritableVector point,
      OutputCollector<Text, WritableVector> collector) throws IOException {
    double minDist = Double.MAX_VALUE;
    Canopy closest = null;
    boolean isCovered = false;
    StringBuilder builder = new StringBuilder();
    for (Canopy canopy : canopies) {
      double dist = measure.distance(canopy.getCenter(), point);
      if (dist < t1) {
        isCovered = true;
        builder.append(canopy.getIdentifier()).append(":");
      } else if (dist < minDist) {
        minDist = dist;
        closest = canopy;
      }
    }
    // if the point is not contained in any canopies (due to canopy centroid
    // clustering), emit the point to the closest covering canopy.
    Text label = isCovered ? new Text(builder.toString()) : closest.getCanopyId();
    collector.collect(key, new LabeledWritableVector(label, point));
  }

  @Override
  public String toString() {
    return getIdentifier() + " - " + getCenter().asFormatString();
  }

  public String getIdentifier() {
    return canopyId.toString();
  }

  public Text getCanopyId() {
    return canopyId;
  }

  /**
   * Return the center point
   * 
   * @return the center of the Canopy
   */
  public WritableVector getCenter() {
    return center;
  }

  /**
   * Return if the point is covered by this canopy
   * 
   * @param point a point
   * @return if the point is covered
   */
  public boolean covers(Vector point) {
    return measure.distance(center, point) < t1;
  }
}
