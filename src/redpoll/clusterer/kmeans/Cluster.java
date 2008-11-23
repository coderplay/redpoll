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

package redpoll.clusterer.kmeans;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import redpoll.core.Vector;
import redpoll.core.WritableSparseVector;
import redpoll.core.WritableVector;
import redpoll.util.DistanceMeasure;

public class Cluster {

  public static final String DISTANCE_MEASURE_KEY = "redpoll.clusterer.kmeans.measure";

  public static final String CLUSTER_PATH_KEY = "redpoll.clusterer.kmeans.path";

  public static final String CLUSTER_CONVERGENCE_KEY = "redpoll.clusterer.kmeans.convergence";

  // this cluster's clusterId
  private final String clusterId;

  // the current center
  private WritableVector center = new WritableSparseVector(0);

  // the current centroid is lazy evaluated and may be null
  private WritableVector centroid = null;

  // the number of points in the cluster
  private int numPoints = 0;

  // the total of all points added to the cluster
  private WritableVector pointTotal = null;

  // has the centroid converged with the center?
  private boolean converged = false;

  private static DistanceMeasure measure;

  private static double convergenceDelta = 0;

  /**
   * Format the cluster for output
   * 
   * @param cluster the Cluster
   * @return
   */
  public static String formatCluster(Cluster cluster) {
    return cluster.getIdentifier() + ": "
        + cluster.computeCentroid().asFormatString();
  }

  /**
   * Configure the distance measure from the job
   * 
   * @param job the JobConf for the job
   */
  public static void configure(JobConf job) {
    try {
      final ClassLoader ccl = Thread.currentThread().getContextClassLoader();
      Class<?> cl = ccl.loadClass(job.get(DISTANCE_MEASURE_KEY));
      measure = (DistanceMeasure) cl.newInstance();
      convergenceDelta = Double.parseDouble(job.get(CLUSTER_CONVERGENCE_KEY));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Configure the distance measure directly. Used by unit tests.
   * 
   * @param aMeasure the DistanceMeasure
   * @param aConvergenceDelta the delta value used to define convergence
   */
  public static void config(DistanceMeasure aMeasure, double aConvergenceDelta) {
    measure = aMeasure;
    convergenceDelta = aConvergenceDelta;
  }

  /**
   * Emit the point to the nearest cluster center
   * 
   * @param point a point
   * @param clusters a List<Cluster> to test
   * @param output the OutputCollector to emit into
   * @throws IOException
   */
  public static void emitPointToNearestCluster(WritableVector point,
      List<Cluster> clusters, OutputCollector<Text, WritableVector> output)
      throws IOException {
    Cluster nearestCluster = null;
    double nearestDistance = Double.MAX_VALUE;
    for (Cluster cluster : clusters) {
      double distance = measure.distance(point, cluster.getCenter());
      if (nearestCluster == null || distance < nearestDistance) {
        nearestCluster = cluster;
        nearestDistance = distance;
      }
    }
    output.collect(new Text(nearestCluster.getIdentifier()), point);
  }

  /**
   * Compute the centroid by averaging the pointTotals
   * 
   * @return the new centroid
   */
  public WritableVector computeCentroid() {
    if (numPoints == 0)
      return pointTotal;
    else if (centroid == null) {
      // lazy compute new centroid
      centroid = (WritableVector) pointTotal.divide(numPoints);
    }
    return centroid;
  }
  
  /**
   * Construct a new cluster with the given point as its center
   * 
   * @param center the center point
   */
  public Cluster(String clusterId, WritableVector center) {
    this.clusterId = clusterId;
    this.center = center;
    this.numPoints = 0;
    this.pointTotal = center.like();
  }

  @Override
  public String toString() {
    return getIdentifier() + " - " + center.asFormatString();
  }

  public String getIdentifier() {
    if (converged)
      return "V" + clusterId;
    else
      return "C" + clusterId;
  }

  /**
   * Add the point to the cluster
   * 
   * @param point a point to add
   */
  public void addPoint(WritableVector point) {
    centroid = null;
    numPoints++;
    if (pointTotal == null)
      pointTotal = point.copy();
    else
      pointTotal = (WritableVector) point.add(pointTotal);
  }

  /**
   * Add the point to the cluster
   * 
   * @param count the number of points in the delta
   * @param delta a point to add
   */
  public void addPoints(int count, WritableVector delta) {
    centroid = null;
    numPoints += count;
    if (pointTotal == null)
      pointTotal = delta.copy();
    else
      pointTotal = (WritableVector) delta.add(pointTotal);
  }

  public WritableVector getCenter() {
    return center;
  }

  public int getNumPoints() {
    return numPoints;
  }

  /**
   * Compute the centroid and set the center to it.
   */
  public void recomputeCenter() {
    center = computeCentroid();
    numPoints = 0;
    pointTotal = center.like();
  }

  /**
   * Return if the cluster is converged by comparing its center and centroid.
   * 
   * @return if the cluster is converged
   */
  public boolean computeConvergence() {
    Vector centroid = computeCentroid();
    converged = measure.distance(centroid, center) <= convergenceDelta;
    return converged;
  }

  public WritableVector getPointTotal() {
    return pointTotal;
  }

  public boolean isConverged() {
    return converged;
  }

}
