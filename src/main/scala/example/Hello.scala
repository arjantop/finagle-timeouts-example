package example

import java.util.concurrent.ThreadLocalRandom

import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.finagle.{Http, Service, SimpleFilter}
import com.twitter.util._
import com.twitter.conversions.time._
import com.twitter.finagle.context.{Contexts, Deadline}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.Logger

object Hello {
  def main(args: Array[String]): Unit = {

    implicit val timer: Timer = DefaultTimer

    val backendService = new Service[Request, Response] {
      private val logger = Logger("BackendService")

      override def apply(request: Request): Future[Response] = {
        val delay = ThreadLocalRandom.current().nextInt(2000)
        logger.info("Delayed for %dms", delay)
        Future.value(Response(Status.Ok)).delayed(delay.millis)
      }
    }
    Http.server.serve(":8989", backendService)

    val expendableLogger = Logger("Expendable")
    def expendable[T](name: String, f: Future[T], default: => T): Future[T] = {
      val r = Deadline.current match {
        case Some(d) =>
          expendableLogger.info("[%s] Using deadline: %s", name, d)
          f.raiseWithin(d.remaining)
        case None => f
      }
      r.rescue {
        case e: Throwable =>
          expendableLogger.warning(e, "[%s] Returning default response", name)
          Future.value(default)
      }
    }

    val backendClient = Http.client
      .withSessionQualifier.noFailureAccrual
      .withRequestTimeout(2.seconds).newService("localhost:8989")

    val mainService = new Service[Request, Response] {
      private val logger = Logger("MainService")

      private def doRequest(name: String): Future[Response] = {
        logger.info("[%s] Calling service", name)
        val elapsed = Stopwatch.start()
        val r = backendClient(Request(Method.Get, "/")).raiseWithin(1.seconds).onSuccess { _ =>
          logger.info("[%s] Call took: %dms", name, elapsed().inMillis)
        }.onFailure { e =>
          logger.error("[%s] Call failed and took: %dms", name, elapsed().inMillis)
        }
        expendable(name, r, default)
      }

      private val default = Response(Status.Accepted)

      override def apply(request: Request): Future[Response] = {
        val c1 = doRequest("request 1")
        val c2 = doRequest("request 2")
        c1.join(c2).flatMap { _ =>
          doRequest("request 3")
        }.onFailure { e =>
          logger.error(e, "Unhandled error")
        }.onSuccess { _ =>
          logger.info("Request done")
        }
      }
    }

    val expendableDeadlineFilter = new SimpleFilter[Request, Response] {
      override def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
        val now = Time.now
        Contexts.broadcast.let(Deadline, Deadline(now, now.plus(1500.millis))) {
          service(request)
        }
      }
    }

    val s = Http.server
      .withRequestTimeout(2.seconds)
      .serve(":8888", expendableDeadlineFilter.andThen(mainService))
    Await.result(s)
  }
}
