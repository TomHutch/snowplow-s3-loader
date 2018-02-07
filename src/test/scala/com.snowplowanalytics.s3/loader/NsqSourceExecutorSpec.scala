package com.snowplowanalytics.s3.loader

import com.snowplowanalytics.s3.loader.model.S3Config
import com.snowplowanalytics.s3.loader.model.S3LoaderConfig

// Specs2
import org.specs2.mutable.Specification
import org.scalatest.{ PrivateMethodTester }

class NsqSourceExecutorSpec extends Specification with PrivateMethodTester {

  "getBaseFilename" should {
    "correctly create default baseFilename" in {
      val s3Config = new S3Config("region", "bucket", "format", 100)
      val s3LoaderConfig = new S3LoaderConfig(null, null, null, null, null, null, s3Config, None)

      val nsqSourceExecutor = new NsqSourceExecutor(s3LoaderConfig, null, null, null, 100, None)

      nsqSourceExecutor.calendar.set(2018, 1, 5) // Month is 0 based

      val getBaseFilenameNsq = PrivateMethod[String]('getBaseFilename)
      val baseFilename = nsqSourceExecutor.invokePrivate(getBaseFilenameNsq(0 : Long, 1 : Long))
      baseFilename must startWith("2018-02-05-00:00:00.000-00:00:00.001")
    }
  }

  "getBaseFilename" should {
    "correctly create baseFilename when partitioningFormat = 'hive'" in {
      val s3Config = new S3Config("region", "bucket", "format", 100, "hive")
      val s3LoaderConfig = new S3LoaderConfig(null, null, null, null, null, null, s3Config, None)

      val nsqSourceExecutor = new NsqSourceExecutor(s3LoaderConfig, null, null, null, 100, None)

      nsqSourceExecutor.calendar.set(2018, 1, 5) // Month is 0 based

      val getBaseFilenameNsq = PrivateMethod[String]('getBaseFilename)
      val baseFilename = nsqSourceExecutor.invokePrivate(getBaseFilenameNsq(0 : Long, 1 : Long))
      baseFilename must startWith("year=2018/month=02/day=05/00:00:00.000-00:00:00.001")
    }
  }

  "getBaseFilename" should {
    "throw an IllegalArgumentException when given an unsupported partitioningFormat" in {
      val s3Config = new S3Config("region", "bucket", "format", 100, "foo")
      val s3LoaderConfig = new S3LoaderConfig(null, null, null, null, null, null, s3Config, None)

      val nsqSourceExecutor = new NsqSourceExecutor(s3LoaderConfig, null, null, null, 100, None)

      nsqSourceExecutor.calendar.set(2018, 1, 5) // Month is 0 based

      val getBaseFilenameNsq = PrivateMethod[String]('getBaseFilename)

      nsqSourceExecutor.invokePrivate(getBaseFilenameNsq(0 : Long, 1 : Long)) must throwA(new IllegalArgumentException(
          "Unsupported partitioning format 'foo'. Check s3.partitioningFormat key in configuration file."
        ))
    }
  }
}
