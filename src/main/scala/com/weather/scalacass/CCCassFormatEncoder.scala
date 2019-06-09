package com.weather.scalacass

import com.weather.scalacass.WhereQueryTags.Implicits.TypedTag
import com.weather.scalacass.WhereQueryTags.WhereQueryTag
import shapeless.labelled.FieldType
import shapeless.ops.hlist.Mapper
import shapeless.{::, <:!<, HList, HNil, LabelledGeneric, Lazy, Poly1, Witness, tag}

abstract class DerivedCCCassFormatEncoder[F] extends CCCassFormatEncoder[F, WhereQueryTag]

object DerivedCCCassFormatEncoder {
  implicit val hNilEncoder: DerivedCCCassFormatEncoder[HNil] = new DerivedCCCassFormatEncoder[HNil] {
    def encodeWithName(f: HNil) = Right(Nil)
    def encodeWithQuery(f: HNil) = Right(Nil)

    val names = Nil
    val types = Nil
  }

  implicit def hConsEncoder[K <: Symbol, H, Tag <: WhereQueryTag : TypedTag, T <: HList](implicit w: Witness.Aux[K], tdH: Lazy[CassFormatEncoder[H, Tag]], tdT: Lazy[DerivedCCCassFormatEncoder[T]]): DerivedCCCassFormatEncoder[FieldType[K, H] :: T] =
    new DerivedCCCassFormatEncoder[FieldType[K, H] :: T] {
      def encodeWithName(f: FieldType[K, H] :: T) = for {
        h <- tdH.value.encode(f.head).right
        t <- tdT.value.encodeWithName(f.tail).right
      } yield (w.value.name.toString, h) :: t
      def encodeWithQuery(f: FieldType[K, H] :: T) = for {
        h <- tdH.value.encode(f.head).right
        t <- tdT.value.encodeWithQuery(f.tail).right
      } yield (tdH.value.withQuery(f.head, w.value.name.toString), h) :: t
      def names = w.value.name.toString :: tdT.value.names
      def types = tdH.value.cassType :: tdT.value.types
    }

  implicit def ccConverter[T, Repr <: HList, Mapr <: HList](implicit gen: LabelledGeneric.Aux[T, Repr],
                                                            hListDecoder: Lazy[DerivedCCCassFormatEncoder[Mapr]],
                                                            mapper: Mapper.Aux[tagFields.type, Repr, Mapr]): DerivedCCCassFormatEncoder[T] =
    new DerivedCCCassFormatEncoder[T] {
      def encodeWithName(f: T) = hListDecoder.value.encodeWithName(gen.to(f).map(tagFields))
      def encodeWithQuery(f: T) = hListDecoder.value.encodeWithQuery(gen.to(f).map(tagFields))
      def names = hListDecoder.value.names
      def types = hListDecoder.value.types
    }

  private object tagFields extends Poly1 {
    import com.weather.scalacass.WhereQueryTags.`=`
    import shapeless.tag.@@

    implicit def atTagged[F, Tag <: WhereQueryTag] = at[F @@ Tag](identity)
    implicit def atNonTagged[F](implicit evNotTagged: F <:!< @@[_, WhereQueryTag]) = at[F](tag[`=`.type](_))
  }
}

trait CCCassFormatEncoder[F, T <: WhereQueryTag] { self =>
  def encodeWithName(f: F): Result[List[(String, AnyRef)]]
  def encodeWithQuery(f: F): Result[List[(String, AnyRef)]]
  def names: List[String]
  def types: List[String]
  def namesAndTypes: List[(String, String)] = names zip types

  final def map[G](fn: G => F): CCCassFormatEncoder[G, T] = new CCCassFormatEncoder[G, T] {
    def encodeWithName(f: G): Result[List[(String, AnyRef)]] = self.encodeWithName(fn(f))
    def encodeWithQuery(f: G): Result[List[(String, AnyRef)]] = self.encodeWithQuery(fn(f))
    def names = self.names
    def types = self.types
  }
  final def flatMap[G](fn: G => Result[F]): CCCassFormatEncoder[G, T] = new CCCassFormatEncoder[G, T] {
    def encodeWithName(f: G): Result[List[(String, AnyRef)]] = fn(f).right.flatMap(self.encodeWithName)
    def encodeWithQuery(f: G): Result[List[(String, AnyRef)]] = fn(f).right.flatMap(self.encodeWithQuery)
    def names = self.names
    def types = self.types
  }
}

object CCCassFormatEncoder extends ProductCCCassFormatEncoders {
  implicit def derive[T, Tag <: WhereQueryTag : TypedTag](implicit derived: Lazy[DerivedCCCassFormatEncoder[T]]): CCCassFormatEncoder[T, Tag] = new CCCassFormatEncoder[T, Tag] {
    override def encodeWithName(f: T): Result[List[(String, AnyRef)]] = derived.value.encodeWithName(f)
    override def encodeWithQuery(f: T): Result[List[(String, AnyRef)]] = derived.value.encodeWithQuery(f)
    override def names: List[String] = derived.value.names
    override def types: List[String] = derived.value.types
  }
  def apply[T, Tag <: WhereQueryTag : TypedTag](implicit instance: CCCassFormatEncoder[T, Tag]): CCCassFormatEncoder[T, Tag] = instance
}
