package com.weather.scalacass

object WhereQueryTags {

  private[scalacass] sealed trait WhereQueryTag {
    val relationSeparator: String
  }

  final case object `=` extends WhereQueryTag {
    override val relationSeparator: String = "="
  }

  final case object <= extends WhereQueryTag {
    override val relationSeparator: String = "<="
  }

  final case object >= extends WhereQueryTag {
    override val relationSeparator: String = ">="
  }

  final case object < extends WhereQueryTag {
    override val relationSeparator: String = "<"
  }

  final case object > extends WhereQueryTag {
    override val relationSeparator: String = ">"
  }

  private[scalacass] object Implicits {
    type TypedTag[T <: WhereQueryTag] = T

    implicit val eq: TypedTag[`=`.type] = `=`
    implicit val lt: TypedTag[<.type] = <
    implicit val gt: TypedTag[>.type] = >
    implicit val lteq: TypedTag[<=.type ] = <=
    implicit val gteq: TypedTag[>=.type] = >=
  }
}

