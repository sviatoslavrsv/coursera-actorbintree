/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  sealed trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
   * in the tree. The actor at reference `requester` should be notified when
   * this operation is completed.
   */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
   * `result` is true if and only if the element is present in the tree.
   */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {

  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => root ! op
    case GC =>
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
  }

  // optional

  /** Handles messages while garbage collection is performed.
   * `newRoot` is the root of the new binary tree where we want to copy
   * all non-removed elements into.
   */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case operation: Operation => pendingQueue = pendingQueue.enqueue(operation)
    case CopyFinished =>
      root ! PoisonPill
      root = newRoot
      context.become(normal)
      pendingQueue.foreach(op => root ! op)
      pendingQueue = Queue.empty[Operation]
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, newElem) =>
      if (newElem > elem) insert(newElem, Right, requester)
      else if (newElem < elem) insert(newElem, Left, requester)
      else removed = false
      requester ! OperationFinished(id)
    case msg@Contains(requester, id, newElem) =>
      val notContained = ContainsResult(id, result = false)
      val result = if (newElem > elem) subtrees.get(Right).map(ref => ref ! msg)
      else if (newElem < elem) subtrees.get(Left).map(ref => ref ! msg)
      else if (newElem == elem) Some(requester ! ContainsResult(id, result = true))
      else Some(requester ! notContained)
      result.getOrElse(requester ! notContained)
    case msg@Remove(requester, id, elemToRemove) =>
      if (elemToRemove > elem) subtrees.get(Right).foreach(ref => ref ! msg)
      else if (elemToRemove < elem) subtrees.get(Left).foreach(ref => ref ! msg)
      else this.removed = true
      requester ! OperationFinished(id)
    case CopyTo(newRoot) =>
      if (!removed) newRoot ! Insert(self, elem, elem)
      else if (subtrees.isEmpty) context.parent ! CopyFinished
      subtrees.values.foreach(_ ! CopyTo(newRoot))
      context.become(copying(subtrees.values.toSet, insertConfirmed = this.removed))
  }

  def insert(newElem: Int, position: Position, requester: ActorRef): Unit = {
    subtrees.get(position) match {
      case Some(_) => requester ! position
      case None =>
        val newNodeActor = context.actorOf(props(newElem, initiallyRemoved = false))
        subtrees = subtrees ++ Map(position -> newNodeActor)
    }
  }

  /** `expected` is the set of ActorRefs whose replies we are waiting for,
   * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
   */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case CopyFinished => context.become(copying(expected - sender(), insertConfirmed))
    case OperationFinished(id) => if (id == elem) context.become(copying(expected, insertConfirmed = true))
  }

}
