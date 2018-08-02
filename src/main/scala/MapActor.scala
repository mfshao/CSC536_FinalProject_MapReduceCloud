import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberUp, UnreachableMember}
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

class MapActor extends Actor {
  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberUp],
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster unsubscribe self

  var reducers: IndexedSeq[ActorRef] = IndexedSeq.empty[ActorRef]
  var jobCounter = 0

  val STOP_WORDS_LIST = List("a", "am", "an", "and", "are", "as", "at", "be",
    "do", "go", "if", "in", "is", "it", "of", "on", "the", "to")

  def receive: PartialFunction[Any, Unit] = {
    case Book(title, url) =>
      println("Received book " + title)
      jobCounter += 1
      process(title, url)
    case ReducerRegistration(m) if !reducers.contains(m) =>
      context watch m
      println("Received new reducer reg: " + m.path)
      reducers = reducers :+ m
    case Terminated(a) =>
      println("Terminated " + a.path)
      reducers = reducers.filterNot(_ == a)
    case Flush =>
      println("Flush broadcasted")
      reducers.foreach(_ ! Flush)
    case MemberUp(m) if m.hasRole("master") =>
      implicit val timeout: Timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
      context.system.actorSelection(RootActorPath(m.address) / "user" / "master").resolveOne().onComplete {
        case Success(actorRef) =>
          actorRef !
            MapperRegistration(self)
          println("Sent MR to " + actorRef.path + " with my ActorRef " + self.path)
      }
    case MemberUp(m) if m.hasRole("reducer") =>
      implicit val timeout: Timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
      context.system.actorSelection(RootActorPath(m.address) / "user" / "reducer").resolveOne().onComplete {
        case Success(actorRef) =>
          if (!reducers.contains(actorRef)) {
            context watch actorRef
            println("New reducer added: " + actorRef)
            reducers = reducers :+ actorRef
            actorRef ! MapperAlive(self)
          }
      }
  }

  // Process book
  def process(title: String, url: String): Unit = {
    val content = getContent(url)
    println("Processing book " + title)
    var namesFound = mutable.HashSet[String]()
    for (word <- content.split("[\\p{Punct}\\s]+")) {
      if ((!STOP_WORDS_LIST.contains(word)) && word(0).isUpper && !namesFound.contains(word)) {
        reducers(jobCounter % reducers.size) forward Word(word, title)
        namesFound += word
      }
    }
  }

  // Get the content at the given URL and return it as a string
  def getContent(url: String): String = {
    //    try {
    Source.fromURL(url).mkString
    //    } catch {     // If failure, just return an empty string
    //      case e: Exception => ""
    //    }
  }
}
