package pl.mpiniarski.distributedmonitor

import pl.mpiniarski.distributedmonitor.communication.Communicator
import pl.mpiniarski.distributedmonitor.communication.Message
import pl.mpiniarski.distributedmonitor.communication.MessageType
import pl.mpiniarski.distributedmonitor.communication.Messenger
import java.util.*
import java.util.concurrent.Semaphore
import kotlin.concurrent.thread


abstract class TimestampedMessage(type : String, val timestamp : Int) : Message(type)

class RequestMessage(timestamp : Int) : TimestampedMessage("REQUEST", timestamp)
class ResponseMessage(timestamp : Int) : TimestampedMessage("RESPONSE", timestamp)
class ReleaseMessage(timestamp : Int) : TimestampedMessage("RELEASE", timestamp)

class Request(val priority : Int, val host : String) : Comparable<Request> {
    override fun compareTo(other : Request) = when {
        priority < other.priority -> -1
        priority > other.priority -> 1
        host < other.host -> -1
        host > other.host -> 1
        else -> 0
    }
}

class DistributedLock(private val node : String, nodes : List<String>, communicator : Communicator) {
    private val messenger = Messenger(
            listOf(
                    MessageType("REQUEST", { val msg = it as RequestMessage; "${msg.timestamp}" }, { RequestMessage(it.toInt()) }),
                    MessageType("RESPONSE", { val msg = it as ResponseMessage; "${msg.timestamp}" }, { ResponseMessage(it.toInt()) }),
                    MessageType("RELEASE", { val msg = it as ReleaseMessage; "${msg.timestamp}" }, { ReleaseMessage(it.toInt()) })
            ),
            communicator
    )
    private val semaphore = Semaphore(0, true)
    private val queue : Queue<Request> = PriorityQueue<Request>()

    private val timeManager = TimeManager(nodes)

    init {
        thread(start = true) {
            while (true) {
                val (sender, message) = messenger.receive()
                val timestampedMessage = message as TimestampedMessage
                val timestamp = timestampedMessage.timestamp
                timeManager.notifySync(timestamp, sender)
                if (message.type == "REQUEST") {
                    val message = message as RequestMessage
                    queue.add(Request(timestamp, sender))
                    messenger.send(sender, ResponseMessage(timeManager.localTime))
                    timeManager.notifyEvent()
                } else if (message.type == "RESPONSE") {
                    val message = message as ResponseMessage
                    val topRequest = queue.element()
                    if (topRequest.host == node && timeManager.allRemoteLaterThen(topRequest.priority)) {
                        semaphore.release()
                    }
                } else if (message.type == "RELEASE") {
                    val message = message as ReleaseMessage
                    queue.remove()
                    if (!queue.isEmpty()) {
                        val topRequest = queue.element()
                        if (topRequest.host == node && timeManager.allRemoteLaterThen(topRequest.priority)) {
                            semaphore.release()
                        }
                    }
                }
            }
        }
    }

    fun lock() {
        queue.add(Request(timeManager.localTime, node))
        messenger.sendToAll(RequestMessage(timeManager.localTime))
        timeManager.notifyEvent()
        semaphore.acquire()
    }

    fun unlock() {
        queue.remove()
        this.messenger.sendToAll(ReleaseMessage(timeManager.localTime))
    }

    fun newCondition() : DistributedCondition {
        //TODO
        return DistributedCondition()
    }
}
