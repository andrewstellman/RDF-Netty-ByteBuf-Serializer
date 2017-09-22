package nettybufrdf

import java.nio.charset.StandardCharsets

import scala.collection.concurrent.TrieMap

import org.eclipse.rdf4j.model.BNode
import org.eclipse.rdf4j.model.IRI
import org.eclipse.rdf4j.model.Literal
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.model.vocabulary.XMLSchema

import io.netty.buffer.ByteBuf

/**
 * Writes RDF4J statements to a Netty byte buffer
 * @param buf Netty byte buffer to write to
 * @param codex Codex provider to look up codes
 */
class ByteBufStatementWriter(buf: ByteBuf, codex: CodexProvider) {

  /** Map to look up BNode IDs from codes (kept in sync with bnodeIdReverseLookup) */
  val bnodeIdLookup: TrieMap[Long, String] = TrieMap[Long, String]()

  /** Map to look up bnode codes from IDs (kept in sync with bnodeIdLookup) */
  val bnodeIdReverseLookup = TrieMap[String, Long]()

  def writerIndex = synchronized { buf.writerIndex }

  /**
   * Writes an RDF4J Statement to the byte buffer
   * @param offset offset in the byte buffer to write the statement
   * @param statement statement to write
   * @param bnodeIdLookup map that holds (and will be updated with) unique long code for each bnode ID
   * @return number of bytes written
   */
  def writeStatement(statement: Statement): Int = {

    try synchronized {
      if (!statement.getContext.isInstanceOf[IRI]) throw new ByteBufRDFException(s"Unable to write statement to byte buffer, non-IRI context is not supported: ${statement}")
      val contextCodeOption = codex.getCode(statement.getContext.asInstanceOf[IRI].stringValue)
      if (contextCodeOption.isEmpty) throw new ByteBufRDFException(s"Unable to write statement to byte buffer, context codex was not found: ${statement}")

      val subjectCodeOption = extractSubjectCode(statement)
      val predicateCodeOption = extractPredicateCode(statement)
      val (objectTypeByte, objectCodeOption) = extractObjectTypeByteAndCode(statement)

      /**
       * Write the Codex or BNode codes for the context, subject, and predicate as longs (24 bytes)
       * and the type of literal/IRI for the object (1 byte)
       * for a total of 25 bytes
       */
      def writeContextSubjectPredicateAndObjectType() = {
        buf.writeLong(contextCodeOption.get)
        buf.writeLong(subjectCodeOption.get)
        buf.writeLong(predicateCodeOption.get)
        buf.writeByte(objectTypeByte)
      }

      if (statement.getObject.isInstanceOf[IRI] || statement.getObject.isInstanceOf[BNode]) {
        val length = 35 // 2 bytes for length + 25 bytes for context/subject/predicate/objecttype + 8 bytes for object IRI/Bnode code  
        buf.writeShort(length)
        writeContextSubjectPredicateAndObjectType()
        buf.writeLong(objectCodeOption.get)
        length

      } else {
        val label = statement.getObject.asInstanceOf[Literal].getLabel
        val labelBytes = label.getBytes(StandardCharsets.UTF_8)
        val length = 29 + labelBytes.length // 2 bytes for length + 25 bytes for context/subject/predicate/objecttype + 2 bytes for label length + # label bytes
        buf.writeShort(length)
        writeContextSubjectPredicateAndObjectType()
        buf.writeShort(labelBytes.length)
        buf.writeBytes(labelBytes)
        length
      }

    } catch {
      case t: Throwable => throw new ByteBufRDFException(s"Unable to write statement to byte buffer: ${statement}", t)
    }

  }

  /** Gets the ID for a BNode to write */
  private def getBNodeId(id: String): Option[Long] = {
    val bnodeId = id
    synchronized {
      val code = -(bnodeIdLookup.size + 1)
      bnodeIdLookup.putIfAbsent(code, bnodeId)
      bnodeIdReverseLookup.putIfAbsent(bnodeId, code)
    }
    bnodeIdReverseLookup.get(bnodeId)
  }

  /** Extracts the Codex code or BNode ID for a Statement's subject */
  private def extractSubjectCode(statement: Statement): Option[Long] = {
    if (statement.getSubject.isInstanceOf[IRI]) {
      val code = codex.getCode(statement.getSubject.asInstanceOf[IRI].stringValue)
      if (code.isEmpty) throw new ByteBufRDFException(s"Unable to write statement to byte buffer, subject codex was not found: ${statement}")
      code

    } else if (statement.getSubject.isInstanceOf[BNode]) {
      getBNodeId(statement.getSubject.asInstanceOf[BNode].getID)

    } else throw new ByteBufRDFException(s"Unable to write statement to byte buffer, non-IRI subject is not supported: ${statement}")
  }

  /** Extracts the Codex code for a Statement's predicate (must be an IRI) */
  private def extractPredicateCode(statement: Statement): Option[Long] = {
    val code = codex.getCode(statement.getPredicate.stringValue)
    if (code.isEmpty) throw new ByteBufRDFException(s"Unable to write statement to byte buffer, predicate codex was not found: ${statement}")
    code
  }

  /** Extracts value for a Statement's object, stored as a byte for the literal type and either an IRI/BNode code or character sequence */
  private def extractObjectTypeByteAndCode(statement: Statement): (Byte, Option[Long]) = {
    val literalTypeByte =
      if (statement.getObject.isInstanceOf[Literal]) {
        val b = XSDLiteralToByte.lookupByte(statement.getObject.asInstanceOf[Literal].getDatatype)
        if (b.isEmpty) throw new ByteBufRDFException(s"Unable to write statement to byte buffer, invalid object literal datatype: ${statement}")
        b.get
      } else XSDLiteralToByte.lookupByte(XMLSchema.ANYURI).get

    val objectCodeOption =
      if (statement.getObject.isInstanceOf[IRI]) {
        val o = codex.getCode(statement.getObject.asInstanceOf[IRI].stringValue)
        if (o.isEmpty) throw new ByteBufRDFException(s"Unable to write statement to byte buffer, object codex was not found: ${statement}")
        o
      } else if (statement.getObject.isInstanceOf[BNode]) {
        getBNodeId(statement.getObject.asInstanceOf[BNode].getID)

      } else None

    (literalTypeByte, objectCodeOption)
  }

}