

package nettybufrdf

trait CodexProvider {
  def getCode(uri: String): Option[Long]
  def getValue(code: Long): Option[String]
}

/**
 * Mock Codex provider
 */
object MockCodexProvider extends CodexProvider {

  private var longToStrings = Map[Long, String]()
  private var stringsToLong = Map[String, Long]()

  def getCode(uri: String): Option[Long] = {
    val code = stringsToLong.getOrElse(uri, (stringsToLong.size + 1).toLong)
    longToStrings += (code -> uri)
    stringsToLong += (uri -> code)
    Some(code)
  }

  def getValue(code: Long): Option[String] = {
    longToStrings.get(code)
  }

}
