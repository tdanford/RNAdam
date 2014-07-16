package org.bdgenomics.RNAdam.algorithms.defuse

import org.apache.spark.rdd.RDD
import org.bdgenomics.RNAdam.models.ReadPair
import org.bdgenomics.formats.avro.ADAMRecord

object Classifier {

  def classify(records: RDD[ADAMRecord]): (RDD[ReadPair], RDD[ReadPair], RDD[ReadPair]) = {

    val r1: RDD[(String, ADAMRecord)] = records.keyBy(x => x.getReadName.toString)
    val aaa: RDD[(String, Iterable[(String, ADAMRecord)])] = r1.groupBy(p => p._1)
    val bbb: RDD[(String, Seq[ADAMRecord])] = aaa.map {
      case (key: String, iter: Iterable[(String, ADAMRecord)]) => (key, iter.map(x => x._2).toSeq)
    }

    val groupedByReadName: RDD[(String, Seq[ADAMRecord])] = r1.groupBy(p => p._1).map {
      case (key: String, iter: Iterable[(String, ADAMRecord)]) => (key, iter.map(x => x._2).toSeq)
    }

    val readPairs: RDD[ReadPair] = groupedByReadName.flatMap {
      case (key: String, records: Seq[ADAMRecord]) =>
        findReadPairs(records)
    }

    /*
      val hasTranscriptName = (record: ADAMRecord) => {
        record.getContig() != null
      }

      val hasTwoTranscripts = (pair: ReadPair) => {
        hasTranscriptName(pair.first) && hasTranscriptName(pair.second)
      }

      val sameTranscript = (pair: ReadPair) => {
        pair.first.getContig.getContigName.equals(pair.second.getContig.getContigName)
      }

      val splitTranscript = (pair: ReadPair) => {
        hasTranscriptName(pair.first) && !hasTranscriptName(pair.second)
      }
*/

    /*
      def concordant: RDD[ReadPair] = readPairs.filter(x => hasTwoTranscripts(x)).filter(x => sameTranscript(x))
      def spanning: RDD[ReadPair] = readPairs.filter(x => hasTwoTranscripts(x)).filter(x => !sameTranscript(x))
*/
    def concordant: RDD[ReadPair] = readPairs.filter(x => sameTranscript(x))
    def spanning: RDD[ReadPair] = readPairs.filter(x => spanningTranscript(x))
    def split: RDD[ReadPair] = readPairs.filter(x => splitTranscript(x))
    (concordant, spanning, split)
  }

  def findReadPairs(records: Seq[ADAMRecord]): Seq[ReadPair] = {

    val firstRecords = records.filter(_.getFirstOfPair)
    val secondRecords = records.filter(_.getSecondOfPair)

    firstRecords.flatMap {
      case first: ADAMRecord =>
        secondRecords.map {
          case second: ADAMRecord =>
            ReadPair(first, second)
        }
    }
  }

  def hasTranscriptName(record: ADAMRecord): Boolean = {
    record.getContig() != null
  }

  def sameTranscript(pair: ReadPair): Boolean = {

    if (!hasTranscriptName(pair.first) || !hasTranscriptName(pair.second))
      return false
    pair.first.getContig.getContigName.equals(pair.second.getContig().getContigName)
  }

  def spanningTranscript(pair: ReadPair): Boolean = {

    if (!hasTranscriptName(pair.first) || !hasTranscriptName(pair.second))
      return false
    !pair.first.getContig.getContigName.equals(pair.second.getContig().getContigName)
  }

  def splitTranscript(pair: ReadPair): Boolean = {
    hasTranscriptName(pair.first) && !hasTranscriptName(pair.second)
  }
}
