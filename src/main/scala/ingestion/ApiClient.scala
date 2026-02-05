package ingestion

import scala.io.Source
import java.net.URL

object ApiClient {

  def get(url: String): String =
    Source.fromURL(new URL(url)).mkString

  def fetchWithRetry(
      url: String,
      maxRetries: Int,
      baseDelayMs: Long
  ): String = {

    var attempt = 0
    var delay = baseDelayMs
    var lastError: Throwable = null

    while (attempt < maxRetries) {
      try {
        return get(url)
      } catch {
        case e: Throwable =>
          lastError = e
          Thread.sleep(delay)
          delay = delay * 2
          attempt += 1
      }
    }

    throw new RuntimeException("API unreachable", lastError)
  }
}
