/**
 * Licensed to the Apache Software Foundation (ASF) under one
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

package redpoll.util;

import redpoll.core.Vector;

/**
 * http://en.wikipedia.org/wiki/Dice_coefficient
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class DiceDistanceMeasure implements DistanceMeasure {

  /**
   * Calculates the distance between two vectors following formula below: <p>
   * 1 - 2|x∩y| / (|x| + |y|)
   * @return 0 for perfect match, > 0 for greater distance
   */
  public double distance(Vector lhs, Vector rhs) {
    int sumSize = lhs.size() + rhs.size();
    if(sumSize <= 0) return 1.0;
    int intersection = 0;
    for (Vector.Element feature : lhs) {
      if(rhs.get(feature.getIndex()) > 0) {
        intersection ++;
      }
    }
    return 1.0 - (2.0 * (double)intersection) / ((double)sumSize);
  }
  
}
