import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, RootActorPath}
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberUp, UnreachableMember}
import akka.cluster.{Cluster, Member}
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

class ReduceActor extends Actor {
  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberUp],
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster unsubscribe self

  var remainingMappers: Int = 0
  var mappers: IndexedSeq[ActorRef] = IndexedSeq.empty[ActorRef]
  var reduceMap: mutable.HashMap[String, List[String]] = mutable.HashMap[String, List[String]]()

  var masterRef: ActorRef = self

  def receive: PartialFunction[Any, Unit] = {
    case Word(word, title) =>
      println("Received Word: " + word)
      if (reduceMap.contains(word)) {
        if (!reduceMap(word).contains(title))
          reduceMap += (word -> (title :: reduceMap(word)))
      }
      else
        reduceMap += (word -> List(title))
    case Flush =>
      println("Received flush")
      remainingMappers -= 1
      if (remainingMappers == 0) {
        println(self.path.toStringWithoutAddress + " : " + reduceMap)
        masterRef ! Done
        println("Sent Done")
        context stop self
      }
    case MapperAlive(m) =>
      if(!mappers.contains(m)) {
        mappers = mappers :+ m
        remainingMappers += 1
        println("Received MapperA, current remaining: " + remainingMappers)
      }
    case MasterAlive(m) =>
      println("Received MA ")
      masterRef = m
      println("Set my master to " + masterRef.path)
    case MemberUp(m) => register(m)
      println("Received member up " + m.roles)
  }

  def register(member: Member): Unit = {
    if (member.hasRole("mapper")) {
      implicit val timeout: Timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
      context.system.actorSelection(RootActorPath(member.address) / "user" / "mapper").resolveOne().onComplete {
        case Success(actorRef) =>
          if(!mappers.contains(actorRef)) {
            remainingMappers += 1
            mappers = mappers :+ actorRef
            println("Received mapper member up, current remaining: " + remainingMappers)
            actorRef !
              ReducerRegistration(self)
            println("Sent RR to " + actorRef.path + " with my ActorRef " + self.path)
          }
      }
    }
    if (member.hasRole("master")) {
      println("Received master member up ")
      implicit val timeout: Timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
      context.system.actorSelection(RootActorPath(member.address) / "user" / "master").resolveOne().onComplete {
        case Success(actorRef) =>
          masterRef = actorRef
          println("Set my master to " + masterRef.path)
      }
    }
  }
}