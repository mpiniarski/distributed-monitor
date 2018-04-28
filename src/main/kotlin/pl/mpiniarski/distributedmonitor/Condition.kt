package pl.mpiniarski.distributedmonitor

import pl.mpiniarski.distributedmonitor.communication.Message
import pl.mpiniarski.distributedmonitor.communication.MessageHeader
import pl.mpiniarski.distributedmonitor.communication.Messenger
import java.util.concurrent.BlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.Semaphore

class ConditionMessage(timestamp : Int, val conditionName : String) : TimestampedMessage(timestamp)

class AwaitRequest(val priority : Int, val host : String) : Comparable<Request> {
    override fun compareTo(other : Request) = when {
        priority < other.priority -> -1
        priority > other.priority -> 1
        host < other.host -> -1
        host > other.host -> 1
        else -> 0
    }
}

class Condition(val name : String, val lock : DistributedLock, val messenger : Messenger,
                val timeManager : TimeManager, val node : String) {

    companion object {
        const val AWAIT_ON_CONDITION : String = "4"
        const val SIGNAL_ON_CONDITION : String = "5"
    }

    internal val semaphore = Semaphore(0, true)

    internal val awaiting : BlockingQueue<AwaitRequest> = PriorityBlockingQueue<AwaitRequest>()


    fun await() {
        awaiting.put(AwaitRequest(timeManager.localTime, node))
        messenger.sendToAll(Message(MessageHeader(node, AWAIT_ON_CONDITION), ConditionMessage(timeManager.localTime, name)))
        timeManager.notifyEvent()
        lock.unlock()
        semaphore.acquire()
        lock.lock()
    }

    fun signal() {
        if (!awaiting.isEmpty()) {
            val topRequest = awaiting.remove()
            messenger.sendToAll(Message(MessageHeader(node, SIGNAL_ON_CONDITION), ConditionMessage(timeManager.localTime, name)))
            if (topRequest.host == node) {
                semaphore.release()
            }
        }
    }
}