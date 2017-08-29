package com.gerritforge.jw2017.etl

import com.gerritforge.jw2017.common.SparkTestSupport
import com.gerritforge.jw2017.model.TestDataFixture
import org.scalatest.{FlatSpec, Matchers}

trait SparkEtlSpec extends FlatSpec with Matchers with SparkTestSupport with TestDataFixture
