package nettybufrdf

import java.nio.charset.StandardCharsets

import scala.collection.concurrent.TrieMap
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.eclipse.rdf4j.model.IRI
import org.eclipse.rdf4j.model.Resource
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.model.Value
import org.eclipse.rdf4j.model.impl.SimpleValueFactory
import org.eclipse.rdf4j.model.vocabulary.XMLSchema

import io.netty.buffer.ByteBuf

/**
 * RDF4J Statement implementation that reads lazy values from a Netty ByteBuf
 * @param buf Netty ByteBuf that contains the statements
 * @param codex Codex code provider to look up codes for IRIs
 * @param bnodeIdReverseLookup Map to look up BNode codes from IDs
 */
class ByteBufStatement(buf: ByteBuf, offset: Int, codex: CodexProvider, bnodeIdReverseLookup: TrieMap[Long, String]) extends Statement {

  /** Some(UTF-8 byte array) if the object is a literal, None if the object is a resource */
  private var objectLabelBytes: Option[Array[Byte]] = None

  /*
   * The following bytes are read from the ByteBuf starting at the offset parameter:
   * 
   * Bytes 0-1: total length in bytes
   * Bytes 2-9: context IRI Codex code
   * Bytes 10-17: subject IRI Codex code from CodexProvider (if positive) or BNode ID from bnodeIdLookup (if negative)
   * Bytes 18-25: predict IRI Codex code
   * Byte 26: type of object (encoded/decoded with XSDLiteralToByte)
   * 
   * If object is IRI or BNode (e.g. type is xsd:anyURI): 
   * Bytes 27-34: object IRI Codex code from CodexProvider (if positive) or BNode ID from bnodeIdLookup (if negative)
   * 
   * If object is literal (any other XML schema type):
   * Bytes 27-28: length of byte array
   * Bytes 29-??: byte array that contains UTF-8 literal label
   */

  /* lazy val fields are used so they aren't read from the byte buffer until the specific Statement impl methods are called */

  /** Total length in bytes */
  lazy val byteLength: Int = buf.getShort(offset)

  /** Context value */
  private lazy val contextValue: Resource = getResourceFromBuffer(offset + 2, "context")

  /** Subject value */
  lazy val subjectValue: Resource = getResourceFromBuffer(offset + 10, "subject")

  /** Predicate value */
  private lazy val predicateValue: IRI = {
    val predicateResource = getResourceFromBuffer(offset + 18, "predicate")
    if (!predicateResource.isInstanceOf[IRI]) throw new ByteBufRDFException("BNode predicate read from byte buffer")
    predicateResource.asInstanceOf[IRI]
  }

  /** Object value */
  private lazy val objectValue: Value =
    try {
      val objectTypeOffset = offset + 26
      val objectValueOffset = offset + 27

      val objectType = XSDLiteralToByte.lookupIRI(buf.getByte(objectTypeOffset))
      if (objectType.isEmpty) throw new ByteBufRDFException("Invalid object type read from byte buffer")
      if (objectType.get == XMLSchema.ANYURI) {
        getResourceFromBuffer(objectValueOffset, "object (IRI)")
      } else {
        try {
          val objectLabelLength = buf.getShort(objectValueOffset)
          val byteArray = new Array[Byte](objectLabelLength)
          buf.getBytes(objectValueOffset + 2, byteArray)
          objectLabelBytes = Some(byteArray)
          SimpleValueFactory.getInstance.createLiteral(new String(byteArray, StandardCharsets.UTF_8), objectType.get)
        } catch {
          case t: Throwable => throw new ByteBufRDFException("Invalid object value read from byte buffer", t)
        }
      }

    } catch {
      case t: Throwable => throw new ByteBufRDFException("Unable to read values from byte buffer", t)
    }

  /** Get an IRI from the byte buffer */
  private def getResourceFromBuffer(offset: Int, description: String): Resource = {
    val code = buf.getLong(offset)

    if (code >= 0) {
      val iriOption = codex.getValue(code)
      if (iriOption.isDefined) {
        Try(SimpleValueFactory.getInstance.createIRI(iriOption.get)) match {
          case Success(iri) => iri
          case Failure(e)   => throw new ByteBufRDFException(s"Invalid ${description} read from byte buffer", e)
        }
      } else throw new ByteBufRDFException(s"Invalid ${description} read from byte buffer")

    } else {
      val bnodeId = bnodeIdReverseLookup.getOrElse(code, s"bnode_${-code}")
      SimpleValueFactory.getInstance.createBNode(bnodeId)
    }
  }

  /* org.eclipse.rdf4j.model.Statement implementation */

  def getContext(): Resource = {
    contextValue
  }

  def getSubject(): Resource = {
    subjectValue
  }

  def getPredicate(): IRI = {
    predicateValue
  }

  def getObject(): Value = {
    objectValue
  }

  override def toString = {
    SimpleValueFactory.getInstance.createStatement(getSubject, getPredicate, getObject, getContext).toString
  }

}
