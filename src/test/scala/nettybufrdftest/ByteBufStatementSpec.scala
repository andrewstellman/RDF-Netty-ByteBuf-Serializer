package nettybufrdftest

import scala.collection.concurrent.TrieMap
import scala.collection.JavaConversions._

import org.eclipse.rdf4j.model.Value
import org.eclipse.rdf4j.model.impl.SimpleValueFactory
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import io.netty.buffer.ByteBufAllocator
import nettybufrdf.ByteBufStatement
import nettybufrdf.ByteBufStatementWriter
import nettybufrdf.MockCodexProvider
import nettybufrdf.ByteBufStatementIterator
import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.rio.ParserConfig
import org.eclipse.rdf4j.rio.Rio
import org.eclipse.rdf4j.model.impl.LinkedHashModel
import org.eclipse.rdf4j.rio.helpers.StatementCollector
import org.eclipse.rdf4j.rio.helpers.ParseErrorCollector
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings

import better.files._
import java.io.FileInputStream
import scala.collection.mutable.Queue
import org.eclipse.rdf4j.model.Statement

import org.log4s._

class ByteBufStatementSpec extends FlatSpec with Matchers {

  private[this] val logger = getLogger

  behavior of "RDF ByteBuf iterator"

  it should "write and read statements to a ByteBuf" in {

    val buf = ByteBufAllocator.DEFAULT.buffer(1024 * 10)
    val w = new ByteBufStatementWriter(buf, MockCodexProvider)

    val vf = SimpleValueFactory.getInstance

    val s1 = vf.createStatement(
      vf.createBNode("this_is_bnode_1"),
      vf.createIRI("http://www.xyz.com/some#predicate"),
      vf.createIRI("http://www.xyz.com/some/object"),
      vf.createIRI("http://www.xyz.com/some/context"))

    val s2 = vf.createStatement(
      vf.createBNode("bnode_two"),
      vf.createIRI("http://www.xyz.com/some#predicate"),
      vf.createLiteral("this is the object").asInstanceOf[Value],
      vf.createIRI("http://www.xyz.com/some/context"))

    val s3 = vf.createStatement(
      vf.createBNode("this_is_bnode_1"),
      vf.createIRI("http://www.xyz.com/some#predicate"),
      vf.createBNode("third_bnode"),
      vf.createIRI("http://www.xyz.com/some/context"))

    val s4 = vf.createStatement(
      vf.createBNode("bnode_two"),
      vf.createIRI("http://www.xyz.com/some#predicate"),
      vf.createBNode("third_bnode"),
      vf.createIRI("http://www.xyz.com/some/context"))

    w.writeStatement(s1)
    w.writeStatement(s2)
    w.writeStatement(s3)
    w.writeStatement(s4)

    val statements = new ByteBufStatementIterator(buf, MockCodexProvider, w.bnodeIdLookup).toList

    statements(0).getSubject should be(s1.getSubject)
    statements(0).getPredicate should be(s1.getPredicate)
    statements(0).getObject should be(s1.getObject)
    statements(0).getContext should be(s1.getContext)

    statements(1).getSubject should be(s2.getSubject)
    statements(1).getPredicate should be(s2.getPredicate)
    statements(1).getObject should be(s2.getObject)
    statements(1).getContext should be(s2.getContext)

    statements(2).getSubject should be(s3.getSubject)
    statements(2).getPredicate should be(s3.getPredicate)
    statements(2).getObject should be(s3.getObject)
    statements(2).getContext should be(s3.getContext)

    statements(3).getSubject should be(s4.getSubject)
    statements(3).getPredicate should be(s4.getPredicate)
    statements(3).getObject should be(s4.getObject)
    statements(3).getContext should be(s4.getContext)

  }

  it should "read a large trig file into a ByteBuf and iterate through it" in {
    val settingsNoVerifyLangTag = new ParserConfig()
    //settingsNoVerifyLangTag.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false)
    val errors = new ParseErrorCollector()
    val parser = Rio.createParser(RDFFormat.TRIG)
    val statementCollector = new StatementCollector(new LinkedHashModel())
    parser.setRDFHandler(statementCollector)

    val file = "src/test/resources/wnba-2015-season.trig".toFile
    logger.info(s"Parsing RDF from ${file.pathAsString}")
    val in = new FileInputStream(file.pathAsString)
    parser.parse(in, "http://www.stellman-greene.com/pbprdf")
    logger.info("Done parsing RDF")

    val buf = ByteBufAllocator.DEFAULT.buffer(1024 * 1024 * 10)
    val w = new ByteBufStatementWriter(buf, MockCodexProvider)

    val statementsFromTrig = statementCollector.getStatements.toList

    var start = System.currentTimeMillis
    var count = 0
    statementsFromTrig.foreach(statement => {
      w.writeStatement(statement)
      count += 1
    })
    logger.info(s"Serizlied ${count} statements to Netty buffer in ${System.currentTimeMillis - start}ms")

    val statementsFromBuffer = Queue[Statement]()
    new ByteBufStatementIterator(buf, MockCodexProvider, w.bnodeIdLookup).toList.foreach(statementsFromBuffer.enqueue(_))

    statementsFromTrig.size should be(statementsFromBuffer.size)

    start = System.currentTimeMillis
    count = 0
    statementsFromTrig.foreach(statementFromTrig => {
      val statementFromBuffer = statementsFromBuffer.dequeue
      count += 1
    })
    logger.info(s"Deserialized ${count} statements to Netty buffer and compared them in ${System.currentTimeMillis - start}ms")
  }
}