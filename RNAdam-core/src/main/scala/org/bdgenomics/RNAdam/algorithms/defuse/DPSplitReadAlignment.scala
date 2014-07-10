/*
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
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
package org.bdgenomics.RNAdam.algorithms.defuse

import scala.math._

class DPSplitReadAlignment(m : Double, u : Double, g : Double,
                           refSeq : String, readSeq : String) {

  val refLength = refSeq.length
  val readLength = readSeq.length

  val array = new Array[Double]( refLength * readLength )

  def get(i : Int, j : Int) : Double = array(i * refLength + j)
  def set(i : Int, j : Int, v : Double) { array(i * refLength + j) = v }

  def delta(i : Char, j : Char) = if(i == j) m else u

  (0 until refLength).foreach { i => set(i, 0, 0.0)  }
  (1 until readLength).foreach { j => set(0, j, get(0, j-1) + g) }

  (1 until refLength).foreach {
    case i =>
      (1 until readLength).foreach {
        case j =>
          val v1 = get(i-1, j-1) + delta(refSeq.charAt(i), readSeq.charAt(j))
          val v2 = get(i-1, j) + g
          val v3 = get(i, j-1) + g
          set(i, j, max(v1, max(v2, v3)))
      }
  }
}
