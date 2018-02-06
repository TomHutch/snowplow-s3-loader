package com.snowplowanalytics.s3.loader

import java.util.Calendar

import com.snowplowanalytics.s3.loader.model.S3Config

// Specs2
import org.specs2.mutable.Specification
import org.scalatest.{ PrivateMethodTester }

class KinesisS3EmitterSpec extends Specification with PrivateMethodTester {

  "getBaseFilename" should {
    "correctly create default baseFilename" in {
      val s3Config = new S3Config("region", "bucket", "format", 100)

      val kinesisS3Emitter = new KinesisS3Emitter(s3Config, null, null, null, 100, None)

      kinesisS3Emitter.calendar.set(2018, 1, 5) // Month is 0 based

      val getBaseFilename = PrivateMethod[String]('getBaseFilename)
      val baseFilename = kinesisS3Emitter.invokePrivate(getBaseFilename("0", "1"))
      baseFilename must_== "2018-02-05-0-1"
    }
  }

  "getBaseFilename" should {
    "correctly create baseFilename when partitioningFormat = 'hive'" in {
      val s3Config = new S3Config("region", "bucket", "format", 100, "hive")

      val kinesisS3Emitter = new KinesisS3Emitter(s3Config, null, null, null, 100, None)

      kinesisS3Emitter.calendar.set(2018, 1, 5) // Month is 0 based

      val getBaseFilename = PrivateMethod[String]('getBaseFilename)
      val baseFilename = kinesisS3Emitter.invokePrivate(getBaseFilename("0", "1"))
      baseFilename must_== "year=2018/month=02/day=05/0-1"
    }
  }

  "getBaseFilename" should {
    "throw an IllegalArgumentException when given an unsupported partitioningFormat" in {
      val s3Config = new S3Config("region", "bucket", "format", 100, "foo")

      val kinesisS3Emitter = new KinesisS3Emitter(s3Config, null, null, null, 100, None)

      kinesisS3Emitter.calendar.set(2018, 1, 5) // Month is 0 based

      val getBaseFilename = PrivateMethod[String]('getBaseFilename)

      kinesisS3Emitter.invokePrivate(getBaseFilename("0", "1")) must throwA(new IllegalArgumentException(
          "Unsupported partitioning format 'foo'. Check s3.partitioningFormat key in configuration file."
        ))
    }
  }
}
