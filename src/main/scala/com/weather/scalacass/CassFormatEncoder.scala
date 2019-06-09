package com.weather.scalacass

import com.datastax.driver.core.DataType
import ScalaSession.UpdateBehavior
import com.weather.scalacass.WhereQueryTags.WhereQueryTag
import com.weather.scalacass.WhereQueryTags.Implicits._

trait CassFormatEncoder[F, Tag <: WhereQueryTag] { self =>
  type From <: AnyRef
  def cassDataType: DataType
  def encode(f: F): Result[From]
  implicit val queryTag: TypedTag[Tag]
  def withQuery(instance: F, name: String) = name + queryTag.relationSeparator + "?"
  def cassType: String = cassDataType.toString

  final def map[G](fn: G => F): CassFormatEncoder[G, Tag] = new CassFormatEncoder[G, Tag] {
    type From = self.From
    val cassDataType = self.cassDataType
    val queryTag = self.queryTag
    def encode(f: G): Result[From] = self.encode(fn(f))
  }

  final def flatMap[G](fn: G => Result[F]): CassFormatEncoder[G, Tag] = new CassFormatEncoder[G, Tag] {
    type From = self.From
    val cassDataType = self.cassDataType
    val queryTag = self.queryTag
    def encode(f: G): Result[From] = fn(f).right.flatMap(self.encode)
  }
}

object CassFormatEncoder extends CassFormatEncoderVersionSpecific {
  type Aux[F, Tag <: WhereQueryTag, From0] = CassFormatEncoder[F, Tag] { type From = From0 }
  def apply[F, Tag <: WhereQueryTag](implicit encoder: CassFormatEncoder[F, Tag]) = encoder

  private[scalacass] def sameTypeCassFormatEncoder[F <: AnyRef, Tag <: WhereQueryTag : TypedTag](_cassDataType: DataType): CassFormatEncoder[F, Tag] =
    new CassFormatEncoder[F, Tag] {
      type From = F
      val cassDataType = _cassDataType
      val queryTag = implicitly[TypedTag[Tag]]
      def encode(f: F) = Right(f)
    }

  private[scalacass] def transCassFormatEncoder[F, F2 <: AnyRef, Tag <: WhereQueryTag : TypedTag](_cassDataType: DataType, _encode: F => F2): CassFormatEncoder[F, Tag] =
    new CassFormatEncoder[F, Tag] {
      type From = F2
      val cassDataType = _cassDataType
      val queryTag = implicitly[TypedTag[Tag]]
      def encode(f: F) = Right[Throwable, From](_encode(f))
    }

  // encoders

  implicit def stringFormat[T <: WhereQueryTag : TypedTag]: CassFormatEncoder[String, T] = sameTypeCassFormatEncoder[String, T](DataType.varchar)
  implicit def uuidFormat[T <: WhereQueryTag : TypedTag]: CassFormatEncoder[java.util.UUID, T] = sameTypeCassFormatEncoder[java.util.UUID, T](DataType.uuid)
  implicit def iNetFormat[T <: WhereQueryTag : TypedTag]: CassFormatEncoder[java.net.InetAddress, T] = sameTypeCassFormatEncoder[java.net.InetAddress, T](DataType.inet)

  implicit def intFormat[T <: WhereQueryTag : TypedTag]: CassFormatEncoder[Int, T] = transCassFormatEncoder(DataType.cint, Int.box)
  implicit def longFormat[T <: WhereQueryTag : TypedTag]: CassFormatEncoder[Long, T] = transCassFormatEncoder(DataType.bigint, Long.box)
  implicit def booleanFormat[T <: WhereQueryTag : TypedTag]: CassFormatEncoder[Boolean, T] = transCassFormatEncoder(DataType.cboolean, Boolean.box)
  implicit def doubleFormat[T <: WhereQueryTag : TypedTag]: CassFormatEncoder[Double, T] = transCassFormatEncoder(DataType.cdouble, Double.box)
  implicit def bigIntegerFormat[T <: WhereQueryTag : TypedTag]: CassFormatEncoder[BigInt, T] = transCassFormatEncoder(DataType.varint, (_: BigInt).underlying)
  implicit def bigDecimalFormat[T <: WhereQueryTag : TypedTag]: CassFormatEncoder[BigDecimal, T] = transCassFormatEncoder(DataType.decimal, (_: BigDecimal).underlying)
  implicit def floatFormat[T <: WhereQueryTag : TypedTag]: CassFormatEncoder[Float, T] = transCassFormatEncoder(DataType.cfloat, Float.box)
  implicit def blobFormat[T <: WhereQueryTag : TypedTag]: CassFormatEncoder[Array[Byte], T] = transCassFormatEncoder(DataType.blob, java.nio.ByteBuffer.wrap)

  def updateBehaviorListEncoder[A, Tag <: WhereQueryTag : TypedTag, UB <: UpdateBehavior[List, A]](implicit underlying: CassFormatEncoder[A, Tag]) = new CassFormatEncoder[UB, Tag] {
    type From = java.util.List[underlying.From]
    val cassDataType = DataType.list(underlying.cassDataType)
    val queryTag = underlying.queryTag
    def encode(f: UB): Result[java.util.List[underlying.From]] = {
      val acc = new java.util.ArrayList[underlying.From]()
      @scala.annotation.tailrec
      def process(l: List[A]): Result[java.util.List[underlying.From]] = l.headOption.map(underlying.encode(_)) match {
        case Some(Left(ff)) => Left(ff)
        case Some(Right(n)) =>
          acc.add(n)
          process(l.tail)
        case None => Right(acc)
      }
      process(f.coll)
    }
    override def withQuery(instance: UB, name: String): String = instance withQuery name
  }

  def updateBehaviorSetEncoder[A, Tag <: WhereQueryTag : TypedTag, UB <: UpdateBehavior[Set, A]](implicit underlying: CassFormatEncoder[A, Tag]) = new CassFormatEncoder[UB, Tag] {
    type From = java.util.Set[underlying.From]
    val cassDataType = DataType.set(underlying.cassDataType)
    val queryTag = underlying.queryTag
    def encode(f: UB): Result[java.util.Set[underlying.From]] = {
      val acc = new java.util.HashSet[underlying.From]()
      @scala.annotation.tailrec
      def process(s: Set[A]): Result[java.util.Set[underlying.From]] = s.headOption.map(underlying.encode(_)) match {
        case Some(Left(ff)) => Left(ff)
        case Some(Right(n)) =>
          acc.add(n)
          process(s.tail)
        case None => Right(acc)
      }
      process(f.coll)
    }
    override def withQuery(instance: UB, name: String): String = instance withQuery name
  }

  implicit def listFormatAdd[A, T <: WhereQueryTag : TypedTag](implicit underlying: CassFormatEncoder[A, T]): CassFormatEncoder[UpdateBehavior.Add[List, A], T] =
    updateBehaviorListEncoder[A, T, UpdateBehavior.Add[List, A]]
  implicit def listFormatSubtract[A, T <: WhereQueryTag : TypedTag](implicit underlying: CassFormatEncoder[A, T]): CassFormatEncoder[UpdateBehavior.Subtract[List, A], T] =
    updateBehaviorListEncoder[A, T, UpdateBehavior.Subtract[List, A]]
  implicit def listFormatReplace[A, T <: WhereQueryTag : TypedTag](implicit underlying: CassFormatEncoder[A, T]): CassFormatEncoder[UpdateBehavior.Replace[List, A], T] =
    updateBehaviorListEncoder[A, T, UpdateBehavior.Replace[List, A]]
  implicit def listFormatUpdateBehavior[A, T <: WhereQueryTag : TypedTag](implicit underlying: CassFormatEncoder[A, T]): CassFormatEncoder[UpdateBehavior[List, A], T] =
    updateBehaviorListEncoder[A, T, UpdateBehavior[List, A]]

  implicit def listFormat[A, T <: WhereQueryTag : TypedTag](implicit underlying: CassFormatEncoder[A, T]): CassFormatEncoder[List[A], T] =
    updateBehaviorListEncoder[A, T, UpdateBehavior.Replace[List, A]].map[List[A]](UpdateBehavior.Replace(_))

  implicit def setFormatAdd[A, T <: WhereQueryTag : TypedTag](implicit underlying: CassFormatEncoder[A, T]): CassFormatEncoder[UpdateBehavior.Add[Set, A], T] =
    updateBehaviorSetEncoder[A, T, UpdateBehavior.Add[Set, A]]
  implicit def setFormatSubtract[A, T <: WhereQueryTag : TypedTag](implicit underlying: CassFormatEncoder[A, T]): CassFormatEncoder[UpdateBehavior.Subtract[Set, A], T] =
    updateBehaviorSetEncoder[A, T, UpdateBehavior.Subtract[Set, A]]
  implicit def setFormatReplace[A, T <: WhereQueryTag : TypedTag](implicit underlying: CassFormatEncoder[A, T]): CassFormatEncoder[UpdateBehavior.Replace[Set, A], T] =
    updateBehaviorSetEncoder[A, T, UpdateBehavior.Replace[Set, A]]
  implicit def setFormatUpdateBehavior[A, T <: WhereQueryTag : TypedTag](implicit underlying: CassFormatEncoder[A, T]): CassFormatEncoder[UpdateBehavior[Set, A], T] =
    updateBehaviorSetEncoder[A, T, UpdateBehavior[Set, A]]

  implicit def setFormat[A, T <: WhereQueryTag : TypedTag](implicit underlying: CassFormatEncoder[A, T]): CassFormatEncoder[Set[A], T] =
    updateBehaviorSetEncoder[A, T, UpdateBehavior.Replace[Set, A]].map[Set[A]](UpdateBehavior.Replace(_))

  implicit def mapFormat[A, B, T <: WhereQueryTag : TypedTag](implicit underlyingA: CassFormatEncoder[A, T], underlyingB: CassFormatEncoder[B, T]): CassFormatEncoder[Map[A, B], T] =
    new CassFormatEncoder[Map[A, B], T] {
      type From = java.util.Map[underlyingA.From, underlyingB.From]
      val cassDataType = DataType.map(underlyingA.cassDataType, underlyingB.cassDataType)
      val queryTag = underlyingA.queryTag
      def encode(f: Map[A, B]): Result[java.util.Map[underlyingA.From, underlyingB.From]] = {
        val acc = new java.util.HashMap[underlyingA.From, underlyingB.From]()
        @scala.annotation.tailrec
        def process(l: Iterable[(A, B)]): Result[java.util.Map[underlyingA.From, underlyingB.From]] = l.headOption.map {
          case (k, v) => for {
            kk <- underlyingA.encode(k).right
            vv <- underlyingB.encode(v).right
          } yield (kk, vv)
        } match {
          case Some(Left(ff)) => Left(ff)
          case Some(Right(n)) =>
            acc.put(n._1, n._2)
            process(l.tail)
          case None => Right(acc)
        }
        process(f)
      }
    }

  implicit def optionFormat[A, T <: WhereQueryTag : TypedTag](implicit underlying: CassFormatEncoder[A, T]): CassFormatEncoder[Option[A], T] = new CassFormatEncoder[Option[A], T] {
    type From = Option[underlying.From]
    val cassDataType = underlying.cassDataType
    val queryTag = underlying.queryTag
    def encode(f: Option[A]): Result[Option[underlying.From]] = f.map(underlying.encode(_)) match {
      case None           => Right(None)
      case Some(Left(_))  => Right(None)
      case Some(Right(n)) => Right(Some(n))
    }
  }
  implicit def eitherFormat[A, T <: WhereQueryTag : TypedTag](implicit underlying: CassFormatEncoder[A, T]): CassFormatEncoder[Result[A], T] = new CassFormatEncoder[Result[A], T] {
    type From = Result[underlying.From]
    val cassDataType = underlying.cassDataType
    val queryTag = underlying.queryTag
    def encode(f: Result[A]) = f.right.map(underlying.encode(_)) match {
      case Left(ff) => Right(Left(ff))
      case other    => other
    }
  }

  implicit val nothingFormat: CassFormatEncoder[Nothing, Nothing] = new CassFormatEncoder[Nothing, Nothing] {
    private def nothingFormatError = throw new IllegalArgumentException("Nothing isn't a real type!")

    type From = Nothing
    lazy val queryTag = nothingFormatError
    def cassDataType = nothingFormatError
    def encode(f: Nothing): Result[From] = nothingFormatError
  }
}
