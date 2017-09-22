package nettybufrdf

import org.eclipse.rdf4j.model.IRI
import org.eclipse.rdf4j.model.vocabulary.XMLSchema
import org.eclipse.rdf4j.model.impl.SimpleValueFactory
import scala.util.Try
import scala.util.Failure
import scala.util.Success

/**
 * Convert XSD literal types to and from bytes
 */
object XSDLiteralToByte {

  /**
   * Convert an IRI to a byte
   * @param iri IRI to convert
   * @return byte value for the IRI, or None if not a known XSD literal type
   */
  def lookupByte(iri: IRI): Option[Byte] = {
    if (iri.getNamespace == XMLSchema.NAMESPACE) {
      typeLookup.get(iri.getLocalName)
    } else None
  }

  /**
   * Convert an IRI string to a byte
   * @param iri IRI string to convert
   * @return byte value for the IRI string, or None if not a known XSD literal type
   */
  def lookupByte(iriString: String): Option[Byte] = {
    Try(SimpleValueFactory.getInstance.createIRI(iriString)) match {
      case Success(iri) => lookupByte(iri)
      case Failure(_)   => None
    }
  }

  /**
   * Convert a byte to an IRI string
   * @param byte byte to convert
   * @return IRI string that corresponds to the byte, or None if not a known byte representation of an XSD literal type
   */
  def lookupString(byte: Byte): Option[String] = {
    byteLookup.get(byte).map(XMLSchema.NAMESPACE + _)
  }

  /**
   * Convert a byte to an IRI string
   * @param byte byte to convert
   * @return IRI that corresponds to the byte, or None if not a known byte representation of an XSD literal type
   */
  def lookupIRI(byte: Byte): Option[IRI] = {
    lookupString(byte).map(SimpleValueFactory.getInstance.createIRI(_))
  }

  /*
   * Lookup lists for literal types derived from RDF4J XMLSchema.class
   */

  private val typeLookup = Map[String, Byte](
    "duration" -> 0,
    "dateTime" -> 1,
    "dayTimeDuration" -> 2,
    "time" -> 3,
    "date" -> 4,
    "gYearMonth" -> 5,
    "gYear" -> 6,
    "gMonthDay" -> 7,
    "gDay" -> 8,
    "gMonth" -> 9,
    "string" -> 10,
    "boolean" -> 11,
    "base64Binary" -> 12,
    "hexBinary" -> 13,
    "float" -> 14,
    "decimal" -> 15,
    "double" -> 16,
    "anyURI" -> 17,
    "QName" -> 18,
    "NOTATION" -> 19,
    "normalizedString" -> 20,
    "token" -> 21,
    "language" -> 22,
    "NMTOKEN" -> 23,
    "NMTOKENS" -> 24,
    "Name" -> 25,
    "NCName" -> 26,
    "ID" -> 27,
    "IDREF" -> 28,
    "IDREFS" -> 29,
    "ENTITY" -> 30,
    "ENTITIES" -> 31,
    "integer" -> 32,
    "long" -> 33,
    "int" -> 34,
    "short" -> 35,
    "byte" -> 36,
    "nonPositiveInteger" -> 37,
    "negativeInteger" -> 38,
    "nonNegativeInteger" -> 39,
    "positiveInteger" -> 40,
    "unsignedLong" -> 41,
    "unsignedInt" -> 42,
    "unsignedShort" -> 43,
    "unsignedByte" -> 44,
    "yearMonthDuration" -> 45)

  private val byteLookup = Map[Byte, String](
    0.byteValue -> "duration",
    1.byteValue -> "dateTime",
    2.byteValue -> "dayTimeDuration",
    3.byteValue -> "time",
    4.byteValue -> "date",
    5.byteValue -> "gYearMonth",
    6.byteValue -> "gYear",
    7.byteValue -> "gMonthDay",
    8.byteValue -> "gDay",
    9.byteValue -> "gMonth",
    10.byteValue -> "string",
    11.byteValue -> "boolean",
    12.byteValue -> "base64Binary",
    13.byteValue -> "hexBinary",
    14.byteValue -> "float",
    15.byteValue -> "decimal",
    16.byteValue -> "double",
    17.byteValue -> "anyURI",
    18.byteValue -> "QName",
    19.byteValue -> "NOTATION",
    20.byteValue -> "normalizedString",
    21.byteValue -> "token",
    22.byteValue -> "language",
    23.byteValue -> "NMTOKEN",
    24.byteValue -> "NMTOKENS",
    25.byteValue -> "Name",
    26.byteValue -> "NCName",
    27.byteValue -> "ID",
    28.byteValue -> "IDREF",
    29.byteValue -> "IDREFS",
    30.byteValue -> "ENTITY",
    31.byteValue -> "ENTITIES",
    32.byteValue -> "integer",
    33.byteValue -> "long",
    34.byteValue -> "int",
    35.byteValue -> "short",
    36.byteValue -> "byte",
    37.byteValue -> "nonPositiveInteger",
    38.byteValue -> "negativeInteger",
    39.byteValue -> "nonNegativeInteger",
    40.byteValue -> "positiveInteger",
    41.byteValue -> "unsignedLong",
    42.byteValue -> "unsignedInt",
    43.byteValue -> "unsignedShort",
    44.byteValue -> "unsignedByte",
    45.byteValue -> "yearMonthDuration")

}