package org.ergoplatform

import akka.actor.{Actor, ActorRef, Props, Stash, Terminated}

import scala.util.{Failure, Success}

object BlockChainNodeActor {
  def props(startBlockChain: BlockChain, knownPeers: Seq[ActorRef]): Props = Props(new BlockChainNodeActor(startBlockChain, knownPeers))

  case object GetBlockChain

  case object GetConnectedPeers

  case class ConnectTo(peer: ActorRef)

  case class NodeScore(score: Long)

  case class GetBlocks(after: Block)

  case class SyncBlocks(blocks: Iterable[Block])

  case class GetNextBlock(block: Block)

  case class NextBlock(next: Option[Block])

}

class BlockChainNodeActor(startBlockChain: BlockChain, knownPeers: Seq[ActorRef]) extends Actor with Stash {

  import BlockChainNodeActor._

  override def preStart(): Unit = {
    knownPeers.foreach { peer =>
      peer ! ConnectTo(self)
    }
    startBlockChain.lastBlock match {
      case Some(b) if knownPeers.nonEmpty =>
        context.become(synchronization(b, knownPeers.head, startBlockChain, knownPeers, 0))
      case _ =>
        context.become(active(startBlockChain, knownPeers))
    }
  }

  private def active(blockChain: BlockChain, peers: Seq[ActorRef]): Receive = {
    case GetConnectedPeers =>
      sender() ! peers
    case GetBlockChain =>
      sender() ! blockChain
    case ConnectTo(node) if !peers.contains(node) =>
      node ! ConnectTo(self)
      node ! NodeScore(blockChain.score)
      context.watch(node)
      context.become(active(blockChain, peers :+ node))

    case NodeScore(score) =>
      if (score > blockChain.score) {
        context.become(synchronization(blockChain.last, sender(), blockChain, peers, score))
      } else if (score != blockChain.score) {
        sender() ! NodeScore(blockChain.score)
      }

    case Terminated(terminatedNode) =>
      context.become(active(blockChain, peers.filter(_ != terminatedNode)))

    case GetNextBlock(b) =>
      if (blockChain.contains(b)) {
        val next = blockChain.dropWhile(_ != b).tail.headOption
        sender() ! NextBlock(next)
      } else {
        sender() ! NextBlock(None)
      }

    case GetBlocks(after) if blockChain.contains(after) =>
      sender() ! SyncBlocks(blockChain.dropWhile(_ != after))

    case b: Block =>
      blockChain.append(b) match {
        case Success(newBlockChain) =>
          peers.filter(_ != sender()).foreach(_ ! b)
          context.become(active(newBlockChain, peers))
        case Failure(f) =>
          peers.foreach(p => p ! NodeScore(blockChain.score))
          println(s"Error on apply block $b to blockchain $blockChain: $f")
      }
  }

  private def synchronization(splitBlock: Block, syncPeer: ActorRef, chain: BlockChain, peers: Seq[ActorRef], score: Long): Receive = {

    syncPeer ! GetNextBlock(splitBlock)

    {
      case NextBlock(Some(next)) if sender() == syncPeer =>
        val right = chain.dropWhile(_ != splitBlock)
        if (right.tail.headOption.contains(next)) {
          val b = right.take(right.size / 2).head
          context.become(synchronization(b, sender(), chain, peers, score))
        } else {
          syncPeer ! GetBlocks(splitBlock)
        }

      case NextBlock(None) if sender() == syncPeer =>
        val left = chain.takeWhile(_ != splitBlock)

        val b = left.take(left.size / 2).headOption.orElse(left.headOption).getOrElse(chain.head)
        context.become(synchronization(b, sender(), chain, peers, score))

      case NodeScore(s) if score < s =>
        context.become(synchronization(chain.last, sender(), chain, peers, scala.math.max(s, score)))

      case ConnectTo(node) if !peers.contains(node) =>
        node ! ConnectTo(self)
        context.watch(node)
        context.become(synchronization(chain.last, syncPeer, chain, peers :+ node, score))

      case SyncBlocks(blocks) if sender() == syncPeer && blocks.headOption.contains(splitBlock) =>

        val c = chain.takeWhile(_ != splitBlock).foldLeft(BlockChain.withGenesis)((c, b) => c.append(b).get)

        val newChain = blocks.foldLeft(c)((c, b) => c.append(b).get)
        peers.foreach(p => p ! NodeScore(newChain.score))
        context.become(active(newChain, peers))
        unstashAll()
      case _ =>
        stash()

    }
  }

  override def receive = Actor.emptyBehavior
}
