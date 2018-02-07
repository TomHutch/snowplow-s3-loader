/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.s3.loader

import scala.util.{Failure, Success, Try}

import java.text.SimpleDateFormat
import java.util.Calendar

import com.snowplowanalytics.s3.loader.model.S3Config

object utils {
    // to rm once 2.12 as well as the right projections
    def fold[A, B](t: Try[A])(ft: Throwable => B, fa: A => B): B = t match {
      case Success(a) => fa(a)
      case Failure(t) => ft(t)
    }

    def createS3Prefix(calendar: Calendar, s3Config: S3Config): String = {
	    val date = calendar.getTime()
	    val dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	    val (yearFormat, monthFormat, dayFormat) = ( new SimpleDateFormat("yyyy"), new SimpleDateFormat("MM"), new SimpleDateFormat("dd"))

	    s3Config.partitioningFormat match {
	      case "flat" => s"${dateFormat.format(date)}-"
	      case "hive" => s"year=${yearFormat.format(date)}/month=${monthFormat.format(date)}/day=${dayFormat.format(date)}/"
	      case s => throw new IllegalArgumentException(s"Unsupported partitioning format '${s}'. Check s3.partitioningFormat key in configuration file.")
	    }
	}
}
