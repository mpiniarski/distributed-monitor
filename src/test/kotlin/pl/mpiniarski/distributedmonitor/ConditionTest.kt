package pl.mpiniarski.distributedmonitor

import junit.framework.Assert.assertEquals
import org.junit.Test
import pl.mpiniarski.distributedmonitor.communication.ZeroMqBinaryMessenger
import java.util.*
import kotlin.collections.ArrayList
import kotlin.concurrent.thread

class ConditionTest {

    companion object {
        const val MAXCOUNT = 1
    }

    @Test
    fun producerConsumerTest() {
        val nodes = listOf(
                "tcp://localhost:5557",
                "tcp://localhost:5558"
        )

        var buffer = 0
        var bufferCount = 0

        val producer = thread(start = true) {
            val communicator = ZeroMqBinaryMessenger(nodes[0], nodes - nodes[0])
            val lock = DistributedLock(nodes[0], nodes - nodes[0], communicator)
            val full = lock.newCondition("full")
            val empty = lock.newCondition("empty")

            for (i in 1 .. 9) {
                Thread.sleep(Random().nextInt(100).toLong())
                lock.lock()
                if (bufferCount == MAXCOUNT) {
                    full.await()
                }
                buffer = i
                bufferCount += 1
                System.out.println("Produced $i")
                if (bufferCount == 1) {
                    empty.signal()
                }
                lock.unlock()
            }
        }

        val result = ArrayList<Int>()

        val consumer = thread(start = true) {
            val communicator = ZeroMqBinaryMessenger(nodes[1], nodes - nodes[1])
            val lock = DistributedLock(nodes[1], nodes - nodes[1], communicator)
            val full = lock.newCondition("full")
            val empty = lock.newCondition("empty")

            for (i in 1 .. 9) {
                Thread.sleep(Random().nextInt(100).toLong())
                lock.lock()
                if (bufferCount == 0) {
                    empty.await()
                }
                bufferCount -= 1
                System.out.println("Consumed $buffer")
                result.add(buffer)
                if (bufferCount == MAXCOUNT - 1) {
                    full.signal()
                }
                lock.unlock()
            }
        }

        producer.join()
        consumer.join()

        assertEquals((1 .. 9).toList(), result)
    }
}