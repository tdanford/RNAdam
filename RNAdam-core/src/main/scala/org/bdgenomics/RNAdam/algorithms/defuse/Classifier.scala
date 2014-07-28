package org.bdgenomics.RNAdam.algorithms.defuse

import org.apache.spark.rdd.RDD
import org.bdgenomics.RNAdam.models.ReadPair
import org.bdgenomics.formats.avro.ADAMRecord

object Classifier {

  def classify(records: RDD[ADAMRecord]): (RDD[ReadPair], RDD[ReadPair], RDD[ReadPair]) = {

    val r1: RDD[(String, ADAMRecord)] = records.keyBy(x => x.getReadName.toString)

    val groupedByReadName: RDD[(String, Seq[ADAMRecord])] = r1.groupBy(p => p._1).map {
      case (key: String, iter: Iterable[(String, ADAMRecord)]) => (key, iter.map(x => x._2).toSeq)
    }

    val readPairs: RDD[ReadPair] = groupedByReadName.flatMap {
      case (key: String, records: Seq[ADAMRecord]) =>
        findReadPairs(records)
    }

    def concordant: RDD[ReadPair] = readPairs.filter(x => sameTranscript(x))
    def spanning: RDD[ReadPair] = readPairs.filter(x => spanningTranscript(x))
    def split: RDD[ReadPair] = readPairs.filter(x => splitTranscript(x))
    (concordant, spanning, split)
  }

  private def findReadPairs(records: Seq[ADAMRecord]): Seq[ReadPair] = {

    val firstRecords: Seq[ADAMRecord] = records.filter(_.getFirstOfPair)
    val secondRecords: Seq[ADAMRecord] = records.filter(_.getSecondOfPair)

    firstRecords.flatMap {
      case first: ADAMRecord =>
        secondRecords.map {
          case second: ADAMRecord =>
            ReadPair(first, second)
        }
    }
  }

  private def hasTranscriptName(record: ADAMRecord): Boolean = {
    record.getContig() != null
  }

  private def sameTranscript(pair: ReadPair): Boolean = {

    if (!hasTranscriptName(pair.first) || !hasTranscriptName(pair.second))
      return false
    pair.first.getContig.getContigName.equals(pair.second.getContig().getContigName)
  }

  private def spanningTranscript(pair: ReadPair): Boolean = {

    if (!hasTranscriptName(pair.first) || !hasTranscriptName(pair.second))
      return false
    !pair.first.getContig.getContigName.equals(pair.second.getContig().getContigName)
  }

  private def splitTranscript(pair: ReadPair): Boolean = {
    hasTranscriptName(pair.first) && !hasTranscriptName(pair.second)
  }
}
