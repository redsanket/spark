/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.lang.Double.longBitsToDouble
import java.lang.Float.intBitsToFloat
import java.math.MathContext

import scala.util.Random

import org.apache.spark.sql.types._

/**
 * Random data generators for Spark SQL DataTypes. These generators do not generate uniformly random
 * values; instead, they're biased to return "interesting" values (such as maximum / minimum values)
 * with higher probability.
 */
object RandomDataGenerator {

  /**
   * The conditional probability of a non-null value being drawn from a set of "interesting" values
   * instead of being chosen uniformly at random.
   */
  private val PROBABILITY_OF_INTERESTING_VALUE: Float = 0.5f

  /**
   * The probability of the generated value being null
   */
  private val PROBABILITY_OF_NULL: Float = 0.1f

  private val MAX_STR_LEN: Int = 1024
  private val MAX_ARR_SIZE: Int = 128
  private val MAX_MAP_SIZE: Int = 128

  /**
   * Helper function for constructing a biased random number generator which returns "interesting"
   * values with a higher probability.
   */
  private def randomNumeric[T](
      rand: Random,
      uniformRand: Random => T,
      interestingValues: Seq[T]): Some[() => T] = {
    val f = () => {
      if (rand.nextFloat() <= PROBABILITY_OF_INTERESTING_VALUE) {
        interestingValues(rand.nextInt(interestingValues.length))
      } else {
        uniformRand(rand)
      }
    }
    Some(f)
  }

  /**
   * Returns a function which generates random values for the given [[DataType]], or `None` if no
   * random data generator is defined for that data type. The generated values will use an external
   * representation of the data type; for example, the random generator for [[DateType]] will return
   * instances of [[java.sql.Date]] and the generator for [[StructType]] will return a
   * [[org.apache.spark.Row]].
   *
   * @param dataType the type to generate values for
   * @param nullable whether null values should be generated
   * @param seed an optional seed for the random number generator
   * @return a function which can be called to generate random values.
   */
  def forType(
      dataType: DataType,
      nullable: Boolean = true,
      seed: Option[Long] = None): Option[() => Any] = {
    val rand = new Random()
    seed.foreach(rand.setSeed)

    val valueGenerator: Option[() => Any] = dataType match {
      case StringType => Some(() => rand.nextString(rand.nextInt(MAX_STR_LEN)))
      case BinaryType => Some(() => {
        val arr = new Array[Byte](rand.nextInt(MAX_STR_LEN))
        rand.nextBytes(arr)
        arr
      })
      case BooleanType => Some(() => rand.nextBoolean())
      case DateType => Some(() => new java.sql.Date(rand.nextInt()))
      case TimestampType => Some(() => new java.sql.Timestamp(rand.nextLong()))
      case DecimalType.Fixed(precision, scale) => Some(
        () => BigDecimal.apply(rand.nextLong, rand.nextInt, new MathContext(precision)))
      case DoubleType => randomNumeric[Double](
        rand, r => longBitsToDouble(r.nextLong()), Seq(Double.MinValue, Double.MinPositiveValue,
          Double.MaxValue, Double.PositiveInfinity, Double.NegativeInfinity, Double.NaN, 0.0))
      case FloatType => randomNumeric[Float](
        rand, r => intBitsToFloat(r.nextInt()), Seq(Float.MinValue, Float.MinPositiveValue,
          Float.MaxValue, Float.PositiveInfinity, Float.NegativeInfinity, Float.NaN, 0.0f))
      case ByteType => randomNumeric[Byte](
        rand, _.nextInt().toByte, Seq(Byte.MinValue, Byte.MaxValue, 0.toByte))
      case IntegerType => randomNumeric[Int](
        rand, _.nextInt(), Seq(Int.MinValue, Int.MaxValue, 0))
      case LongType => randomNumeric[Long](
        rand, _.nextLong(), Seq(Long.MinValue, Long.MaxValue, 0L))
      case ShortType => randomNumeric[Short](
        rand, _.nextInt().toShort, Seq(Short.MinValue, Short.MaxValue, 0.toShort))
      case NullType => Some(() => null)
      case ArrayType(elementType, containsNull) => {
        forType(elementType, nullable = containsNull, seed = Some(rand.nextLong())).map {
          elementGenerator => () => Array.fill(rand.nextInt(MAX_ARR_SIZE))(elementGenerator())
        }
      }
      case MapType(keyType, valueType, valueContainsNull) => {
        for (
          keyGenerator <- forType(keyType, nullable = false, seed = Some(rand.nextLong()));
          valueGenerator <-
            forType(valueType, nullable = valueContainsNull, seed = Some(rand.nextLong()))
        ) yield {
          () => {
            Seq.fill(rand.nextInt(MAX_MAP_SIZE))((keyGenerator(), valueGenerator())).toMap
          }
        }
      }
      case StructType(fields) => {
        val maybeFieldGenerators: Seq[Option[() => Any]] = fields.map { field =>
          forType(field.dataType, nullable = field.nullable, seed = Some(rand.nextLong()))
        }
        if (maybeFieldGenerators.forall(_.isDefined)) {
          val fieldGenerators: Seq[() => Any] = maybeFieldGenerators.map(_.get)
          Some(() => Row.fromSeq(fieldGenerators.map(_.apply())))
        } else {
          None
        }
      }
      case unsupportedType => None
    }
    // Handle nullability by wrapping the non-null value generator:
    valueGenerator.map { valueGenerator =>
      if (nullable) {
        () => {
          if (rand.nextFloat() <= PROBABILITY_OF_NULL) {
            null
          } else {
            valueGenerator()
          }
        }
      } else {
        valueGenerator
      }
    }
  }
}
