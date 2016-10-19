package com.databricks.spark.xml

import java.io.{IOException, InputStream}
import java.nio.charset.Charset

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Seekable
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.mapred.TextInputFormat

// remove if not needed


class MRv1XmlInputFormat extends TextInputFormat {
  override def getRecordReader(
      inputSplit: InputSplit,
      jobConf: JobConf,
      reporter: Reporter): RecordReader[LongWritable, Text] = {
    new MRv1XmlRecordReader(inputSplit, jobConf)
  }
}

object MRv1XmlInputFormat {
  /** configuration key for start tag */
  val START_TAG_KEY: String = "xmlinput.start"
  /** configuration key for end tag */
  val END_TAG_KEY: String = "xmlinput.end"
  /** configuration key for encoding type */
  val ENCODING_KEY: String = "xmlinput.encoding"
}

private[xml] class MRv1XmlRecordReader(split: InputSplit,
                                       jobConf: JobConf) extends RecordReader[LongWritable, Text] {
  private var startTag: Array[Byte] = _
  private var currentStartTag: Array[Byte] = _
  private var endTag: Array[Byte] = _
  private var space: Array[Byte] = _
  private var angleBracket: Array[Byte] = _

//  private var currentKey: LongWritable = _
//  private var currentValue: Text = _

  private var start: Long = _
  private var end: Long = _
  private var in: InputStream = _
  private var filePosition: Seekable = _
  private var decompressor: Decompressor = _

  private val buffer: DataOutputBuffer = new DataOutputBuffer

  //    val fileSplit: FileSplit = split.asInstanceOf[FileSplit]
  //    val file = fileSplit.getPath
  //    val fs = file.getFileSystem(jobConf)
  //    private val startTag = jobConf.get(MRv1XmlInputFormat.START_TAG_KEY).getBytes("utf-8")
  //    private val endTag = jobConf.get(MRv1XmlInputFormat.END_TAG_KEY).getBytes("utf-8")
  //    private val start = fileSplit.getStart
  //    private val end = start + split.getLength
  //    private val fsin = fs.open(fileSplit.getPath)
  //    private val buffer = new DataOutputBuffer()
  //
  //    fsin.seek(start)
  def initialize(split: InputSplit, conf: JobConf): Unit = {
    val fileSplit: FileSplit = split.asInstanceOf[FileSplit]
    val charset =
      Charset.forName(conf.get(XmlInputFormat.ENCODING_KEY, XmlOptions.DEFAULT_CHARSET))
    startTag = conf.get(XmlInputFormat.START_TAG_KEY).getBytes(charset)
    endTag = conf.get(XmlInputFormat.END_TAG_KEY).getBytes(charset)
    space = " ".getBytes(charset)
    angleBracket = ">".getBytes(charset)
    require(startTag != null, "Start tag cannot be null.")
    require(endTag != null, "End tag cannot be null.")
    require(space != null, "White space cannot be null.")
    require(angleBracket != null, "Angle bracket cannot be null.")
    start = fileSplit.getStart
    end = start + fileSplit.getLength

    // open the file and seek to the start of the split
    val path = fileSplit.getPath
    val fs = path.getFileSystem(conf)
    val fsin = fs.open(fileSplit.getPath)

    val codec = new CompressionCodecFactory(conf).getCodec(path)
    if (null != codec) {
      decompressor = CodecPool.getDecompressor(codec)
      codec match {
        case sc: SplittableCompressionCodec =>
          val cIn = sc.createInputStream(
            fsin,
            decompressor,
            start,
            end,
            SplittableCompressionCodec.READ_MODE.BYBLOCK)
          start = cIn.getAdjustedStart
          end = cIn.getAdjustedEnd
          in = cIn
          filePosition = cIn
        case c: CompressionCodec =>
          if (start != 0) {
            // So we have a split that is only part of a file stored using
            // a Compression codec that cannot be split.
            throw new IOException("Cannot seek in " +
              codec.getClass.getSimpleName + " compressed stream")
          }
          val cIn = c.createInputStream(fsin, decompressor)
          in = cIn
          filePosition = fsin
      }
    } else {
      in = fsin
      filePosition = fsin
      filePosition.seek(start)
    }
  }

  initialize(split, jobConf)

  /**
    * Finds the start of the next record.
    * It treats data from `startTag` and `endTag` as a record.
    *
    * @param key the current key that will be written
    * @param value  the object that will be written
    * @return whether it reads successfully
    */
  override def next(key: LongWritable, value: Text): Boolean = {
    if (readUntilStartElement()) {
      try {
        buffer.write(currentStartTag)
        if (readUntilEndElement()) {
          key.set(filePosition.getPos)
          value.set(buffer.getData, 0, buffer.getLength)
          true
        } else {
          false
        }
      } finally {
        buffer.reset
      }
    } else {
      false
    }
  }

  private def readUntilStartElement(): Boolean = {
    currentStartTag = startTag
    var i = 0
    while (true) {
      val b = in.read()
      if (b == -1 || (i == 0 && filePosition.getPos > end)) {
        // End of file or end of split.
        return false
      } else {
        if (b == startTag(i)) {
          if (i >= startTag.length - 1) {
            // Found start tag.
            return true
          } else {
            // In start tag.
            i += 1
          }
        } else {
          if (i == (startTag.length - angleBracket.length) && checkAttributes(b)) {
            // Found start tag with attributes.
            return true
          } else {
            // Not in start tag.
            i = 0
          }
        }
      }
    }
    // Unreachable.
    false
  }

  private def readUntilEndElement(): Boolean = {
    var si = 0
    var ei = 0
    var depth = 0
    while (true) {
      val b = in.read()
      if (b == -1) {
        // End of file (ignore end of split).
        return false
      } else {
        buffer.write(b)
        if (b == startTag(si) && b == endTag(ei)) {
          // In start tag or end tag.
          si += 1
          ei += 1
        } else if (b == startTag(si)) {
          if (si >= startTag.length - 1) {
            // Found start tag.
            si = 0
            ei = 0
            depth += 1
          } else {
            // In start tag.
            si += 1
            ei = 0
          }
        } else if (b == endTag(ei)) {
          if (ei >= endTag.length - 1) {
            if (depth == 0) {
              // Found closing end tag.
              return true
            } else {
              // Found nested end tag.
              si = 0
              ei = 0
              depth -= 1
            }
          } else {
            // In end tag.
            si = 0
            ei += 1
          }
        } else {
          // Not in start tag or end tag.
          si = 0
          ei = 0
        }
      }
    }
    // Unreachable.
    false
  }

  private def checkAttributes(current: Int): Boolean = {
    var len = 0
    var b = current
    while(len < space.length && b == space(len)) {
      len += 1
      if (len >= space.length) {
        currentStartTag = startTag.take(startTag.length - angleBracket.length) ++ space
        return true
      }
      b = in.read
    }
    false
  }


  override def createKey(): LongWritable = new LongWritable()

  override def createValue(): Text = new Text()

  override def getPos(): Long = filePosition.getPos

  override def close(): Unit = {
    try {
      if (in != null) {
        in.close()
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor)
        decompressor = null
      }
    }
  }

  override def getProgress: Float = (filePosition.getPos - start) / (end - start).toFloat

}
