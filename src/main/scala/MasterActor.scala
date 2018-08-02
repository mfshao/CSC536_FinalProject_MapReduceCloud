import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, RootActorPath, Terminated}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberUp, UnreachableMember}
import akka.util.Timeout

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Success


/////////////
import akka.actor.Actor
////////////

class MasterActor extends Actor {
  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberUp],
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster unsubscribe self

  var mappers: IndexedSeq[ActorRef] = IndexedSeq.empty[ActorRef]
  var jobCounter = 0

  context.system.scheduler.scheduleOnce(30000 milliseconds) {
    println("Sending books to mappers")
    self ! Book("A Tale of Two Cities", "http://reed.cs.depaul.edu/lperkovic/csc536/homeworks/gutenberg/pg98.txt")
    self ! Book("A Christmas Carol", "http://reed.cs.depaul.edu/lperkovic/csc536/homeworks/gutenberg/pg19337.txt")
  }
  context.system.scheduler.scheduleOnce(60000 milliseconds) {
    println("Sending flush")
    self ! Flush
  }

  def receive: PartialFunction[Any, Unit] = {
    case Terminated(a) =>
      println("Terminated " + a.path)
      mappers = mappers.filterNot(_ == a)
    case Flush =>
      println("Flush broadcasted")
      mappers.foreach(_ ! Flush)
    case msg: Book =>
      if (mappers.isEmpty){
        println("Service unavailable, try again later")
      }
      else {
        println("Books sent to mappers")
        jobCounter += 1
        mappers(jobCounter % mappers.size) forward msg
      }
    case MemberUp(m) if m.hasRole("mapper") =>
      println("Received mapper MemberUp")
      implicit val timeout: Timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
      context.system.actorSelection(RootActorPath(m.address) / "user" / "mapper").resolveOne().onComplete {
        case Success(actorRef) => if (!mappers.contains(actorRef)) {
          context watch actorRef
          mappers = mappers :+ actorRef
          println("New mapper added: " + actorRef.path)
        }
      }
    case MemberUp(m) if m.hasRole("reducer") =>
      println("Received reducer MemberUp")
      implicit val timeout: Timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
      context.system.actorSelection(RootActorPath(m.address) / "user" / "reducer").resolveOne().onComplete {
        case Success(actorRef) => {
          actorRef ! MasterAlive(self)
          println("Sent MA to: " + actorRef.path)
        }
      }
    case MapperRegistration(m) if !mappers.contains(m) =>
      context watch m
      println("Received new mapper reg: " + m.path)
      mappers = mappers :+ m
      println("New mapper added: " + m.path)
    case Done =>
      println("Received Done from" + sender)
  }
}
