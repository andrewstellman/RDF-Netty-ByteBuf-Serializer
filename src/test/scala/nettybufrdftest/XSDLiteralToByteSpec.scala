package nettybufrdftest

import org.scalatest._
import io.netty.buffer.ByteBufAllocator
import java.nio.charset.StandardCharsets
import org.eclipse.rdf4j.model.impl.SimpleValueFactory
import org.eclipse.rdf4j.model.vocabulary.XMLSchema
import nettybufrdf.XSDLiteralToByte

class XSDLiteralToByteSpec extends FlatSpec with Matchers {

  behavior of "XSDLiteralToByte"

  it should "convert XSD IRIs to and from bytes" in {
    val dayTimeDuration: Byte = XSDLiteralToByte.lookupByte(XMLSchema.DAYTIMEDURATION).get
    val string: Byte = XSDLiteralToByte.lookupByte(XMLSchema.STRING.stringValue).get
    
    dayTimeDuration.toInt should be >= 0
    string.toInt should be >= 0

    XSDLiteralToByte.lookupIRI(dayTimeDuration) should be(Some(XMLSchema.DAYTIMEDURATION))
    XSDLiteralToByte.lookupString(string) should be(Some(XMLSchema.STRING.stringValue))
  }

  it should "handle unknown IRIs or bytes" in {
    XSDLiteralToByte.lookupByte(SimpleValueFactory.getInstance.createIRI("http://www.google.com")) should be(None)
    XSDLiteralToByte.lookupIRI(-1) should be(None)
    XSDLiteralToByte.lookupString(-23) should be(None)
  }

}