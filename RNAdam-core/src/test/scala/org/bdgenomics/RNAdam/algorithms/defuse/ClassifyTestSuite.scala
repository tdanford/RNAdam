/**
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

import org.apache.spark.rdd.RDD
import org.bdgenomics.RNAdam.models.ReadPair
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro.{ ADAMRecord, ADAMContig }

class ClassifyTestSuite extends SparkFunSuite {

  sparkTest("Basic sanity check") {

  def rec(readName: String, contigName: String, readMapped: Boolean = true, secondOfPair: Boolean = false): ADAMRecord = {
    val contig = ADAMContig.newBuilder().setContigName(contigName).build()
    ADAMRecord.newBuilder()
      .setReadName(readName)
      .setContig(contig)
      .setReadMapped(readMapped)
      .setFirstOfPair(!secondOfPair)
      .setSecondOfPair(secondOfPair)
      .build()
  }

  sparkTest("Basic sanity check") {

    val rn1cn1_a = rec("readName1", "chr1")
    val rn1cn1_b = rec("readName1", "chr1", secondOfPair = true)
    val rn1cn2_a = rec("readName1", "chr2", secondOfPair = true)
    val rn2cn1_a = rec("readName2", "chr1", secondOfPair = true)
    val rn2cnNull_a = rec("readName1", null, readMapped = false, secondOfPair = true)

    val defuse = new Defuse(new GreedyVertexCover(), 0 /*alpha*/ )

    val recordsRdd = sc.parallelize(Seq(rn1cn1_a, rn1cn1_b, rn1cn2_a, rn2cn1_a, rn2cnNull_a))

    val (concordant: RDD[ReadPair], spanning: RDD[ReadPair], split: RDD[ReadPair]) = defuse.classify(recordsRdd)

    //println("count concordant: " + concordant.count())
    //println("count spanning: " + spanning.count)
    //println("count split: " + split.count)
    assert(concordant.count === 1)
    assert(spanning.count === 1)
    assert(split.count === 1)

    concordant.map {
      case pair: ReadPair =>
        assert(pair.first.getReadName === pair.second.getReadName)
        assert(pair.first.getContig === pair.second.getContig)
    }

    spanning.map {
      case pair: ReadPair =>
        assert(pair.first.getReadName === pair.second.getReadName)
        assert(pair.first.getContig != pair.second.getContig)
    }

    split.map {
      case pair: ReadPair =>
        assert(pair.first.getReadName === pair.second.getReadName)
        assert(pair.first.getContig != null && pair.second.getContig == null)
    }

  }
}
