package nettybufrdf

import io.netty.buffer.ByteBuf
import org.eclipse.rdf4j.model.Statement
import scala.collection.concurrent.TrieMap

/**
 * Iterator that reads statements written to a Netty ByteBuf
 * @param buf Netty ByteBuf that contains the statements
 * @param codex Codex code provider to look up codes for IRIs
 * @param bnodeIdReverseLookup Map to look up BNode IDs from codes
 */
class ByteBufStatementIterator(buf: ByteBuf, codex: CodexProvider, bnodeIdReverseLookup: TrieMap[Long, String]) extends Iterator[Statement] {

  private var bytesRead = 0

  def hasNext: Boolean = {
    bytesRead < buf.writerIndex
  }

  def next(): Statement = synchronized {
    val statement = new ByteBufStatement(buf, bytesRead, codex, bnodeIdReverseLookup)
    bytesRead += statement.byteLength
    statement
  }

}
