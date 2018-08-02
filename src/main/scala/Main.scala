import java.util

import akka.actor._
import com.typesafe.config.ConfigFactory

object Main extends App {

  val config = ConfigFactory.load()
  val roles: util.List[String] = config.getStringList("akka.cluster.roles")
  val system = ActorSystem("ClusterSystem", config)

  if (roles.contains("master")){
    system.actorOf(Props[MasterActor], "master")
  }
  if (roles.contains("mapper")){
    system.actorOf(Props[MapActor], "mapper")
  }
  if (roles.contains("reducer")){
    system.actorOf(Props[ReduceActor], "reducer")
  }
}