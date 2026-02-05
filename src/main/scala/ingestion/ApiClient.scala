package ingestion

import scala.io.Source
import java.net.URL

object ApiClient {

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
        return Source.fromURL(new URL(url)).mkString
      } catch {
        case e: Throwable =>
          lastError = e
          Thread.sleep(delay)
          delay = delay * 2 // backoff exponentiel
          attempt += 1
      }
    }

    throw new RuntimeException("API unreachable", lastError)
  }
}
