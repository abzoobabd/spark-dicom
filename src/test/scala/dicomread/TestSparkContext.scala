package dicomread

import org.apache.spark.sql.{SparkSession,SQLImplicits,SQLContext}
import org.apache.spark.SparkContext

import scala.transient
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait TestSparkContext extends BeforeAndAfterAll {self:FunSuite =>
  @transient var spark:SparkSession = _
  @transient var sc:SparkContext = _

  override def beforeAll(): Unit ={
    super.beforeAll()
    spark = SparkSession.builder().master("local").appName("DicomSchemaTest").getOrCreate()
    sc = spark.sparkContext
  }

  override def afterAll() {
    try{
      SparkSession.clearActiveSession()
      if(spark!=null){
        spark.stop()
      }
      spark=null
    }
    finally {
      super.afterAll()
    }
  }

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

}
