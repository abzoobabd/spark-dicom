package dicomread

import org.scalatest.FunSuite
import dicomread.DicomSchema.readDicom

class TestDicomSchemaSuite extends FunSuite with TestSparkContext {

  private lazy val dicomImagePath = getClass.getResource("/dicomimages/dcm").getPath

  test("Smoke test : create basic spark data frame."){
    val df = spark.createDataFrame(Seq((0,0.0)))
    assert(df.count() == 1)
  }

  test("readDicom count test."){
    val (df,cf) = readDicom(dicomImagePath,spark)
    assert(df.count() == 6)
    assert(cf.count() == 2)
  }

  test("readDicom test : spark session is null."){
    val (df,cf) = readDicom(dicomImagePath,null)
    assert(df.count() == 6)
  }

  test("readDicom test : spark session is not null."){
    val (df,cf) = readDicom(dicomImagePath,spark)
    assert(df.count() == 6)
  }

  test("readDicom test : A non-dicom file shold not be read into the data frame."){
    val (df,cf) = readDicom(dicomImagePath,spark)
    assert(df.count() == 6)
  }

  test("readDicom test: a jpg image is passed and it should not be stored in a data frame."){
    val (df,cf) = readDicom(dicomImagePath,spark)
    assert(df.count() == 6)
  }

  test("readDicom test: default partition test"){
    val (df,cf) = readDicom(dicomImagePath,spark)
    assert(df.rdd.getNumPartitions === spark.sparkContext.defaultParallelism)
  }

  test("readDicom test : partition is equal to zero."){
    val (df,cf) = readDicom(dicomImagePath,spark,0)
    assert(df.rdd.getNumPartitions === spark.sparkContext.defaultParallelism)
  }

  test("readDicom test: partitions test"){
    val (df,cf) = readDicom(dicomImagePath,spark,3)
    assert(df.rdd.getNumPartitions === 3)
  }
}