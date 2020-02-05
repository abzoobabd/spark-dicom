/*
   Copyright 2020 Rahul Lalwani

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package dicomread

import java.io._
import java.io.ByteArrayInputStream

import org.apache.commons.io.IOUtils
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dcm4che3.io.DicomInputStream
import org.json._

object DicomSchema {

  /**
   * Get the metadata from DicomInputStream represented as Array of Bytes
   *
   * @param byteArray DicomInputStream represented as Array of Bytes
   * @return          metadata in json format as string
   * */
  def getMetaDataJson(byteArray: Array[Byte]): String = {

    val metaDataInputStream = new DataInputStream(new ByteArrayInputStream(byteArray))
    val metaDataDicomInputStream = new DicomInputStream(metaDataInputStream)
    val metaDataOutputStream = new ByteArrayOutputStream()
    try
      {
        val dcm2Xml = new Dcm2Xml()
        dcm2Xml.convert(metaDataDicomInputStream, metaDataOutputStream)
        val xmlStr = metaDataOutputStream.toString()

        val jsonObject = XML.toJSONObject(xmlStr)
        jsonObject.toString()
      }
    catch
      {
        case e:Exception => e.toString()
      }
    finally
    {
      metaDataInputStream.close()
      metaDataDicomInputStream.close()
      metaDataOutputStream.close()
    }

  }
  /**
   * Utility function to read dicom images in a dataframe.
   *
   * @param file     File path on the hdfs or local file system.
   * @param fileData File data as a PortableDataStream.
   * @return         Tuple containing filename,metadata and pixeldata or corrupted file name and its corresponding error.
   */
  def importDcm(file: String, fileData: PortableDataStream):Either[(String,String),(String,String,Array[Byte])] = {

    val fileInputStream = fileData.open()
    try
      {
        val byteArray = IOUtils.toByteArray(fileInputStream)
        val metaData = getMetaDataJson(byteArray)
        Right((file, metaData, byteArray))
      }
    catch
      {
        case e:Exception => Left((file,e.toString()))
      }
    finally
    {
      fileInputStream.close()
    }
  }
  /**
   *
   * @param path          Path to the dicom images directory.
   * @param sparkSession  Spark Session
   * @param numPartitions Number of data frame partitions.
   * @return              DataFrame containing three columns named Origin,Metadata and Pixeldata and DataFrame containing corrupt files data.
   */
  def readDicom (path: String, sparkSession: SparkSession, numPartitions: Int = -1):(DataFrame,DataFrame) = {

    val session = if (sparkSession != null) sparkSession else SparkSession.builder().getOrCreate
    val partitions =
      if (numPartitions > 0) {
        numPartitions
      } else {
        session.sparkContext.defaultParallelism
      }
    //println("Number of partitons is :" + partitions)
    var result:DataFrame = null
    var corrupt:DataFrame = null
    try
      {
        val binResult = session.sparkContext.binaryFiles(path,partitions)
        val binaryDataRdd = if (numPartitions <= 0) binResult else binResult.repartition(partitions)
        val importDcmRdd:RDD[Either[(String,String),(String,String,Array[Byte])]] = binaryDataRdd.map(x => importDcm(x._1, x._2))
        val metaDataRdd:RDD[(String,String,Array[Byte])] = importDcmRdd.filter(_.isRight).map(_.right.get)
        val exceptionRdd:RDD[(String,String)] = importDcmRdd.filter(_.isLeft).map(_.left.get)
        import session.implicits._
        result = metaDataRdd.toDF("Origin","MetaData","PixelData")
        corrupt = exceptionRdd.toDF("Origin","Exception")
      }
    catch
      {
        case e:Exception => println(e.toString())
      }
    (result,corrupt)
  }
}