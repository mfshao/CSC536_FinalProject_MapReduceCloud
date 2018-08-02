import akka.actor.ActorRef

case class Book(title: String, url: String)

case class Word(word: String, title: String)

case class ReducerRegistration(m :ActorRef)

case class MapperRegistration(m :ActorRef)

case class MasterAlive(m :ActorRef)

case class MapperAlive(m :ActorRef)

case object Flush

case object Done
