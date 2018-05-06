package pl.mpiniarski.distributedmonitor

import junit.framework.Assert.assertEquals
import org.junit.Test
import pl.mpiniarski.distributedmonitor.communication.Messenger
import pl.mpiniarski.distributedmonitor.communication.ZeroMqBinaryMessenger
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.ArrayList
import kotlin.concurrent.thread

class DistributedConditionTest {
    companion object {
        const val MAXCOUNT = 1
    }

    @Test
    fun producerConsumerTest() {
        val nodes = listOf(
                "localhost:5557",
                "localhost:5558"
        )

        var buffer = 0
        var bufferCount = 0

        val itemsRange = 1 .. 9

        val producerBinaryMessenger = ZeroMqBinaryMessenger(nodes[0], nodes - nodes[0])
        val producerMessenger = Messenger(producerBinaryMessenger)
        val producer = thread(start = true) {
            val lock = DistributedLock("distributedLock", producerMessenger, ReentrantLock(), TimeManager(nodes - nodes[0]))
            val full = lock.newCondition("full")
            val empty = lock.newCondition("empty")
            producerMessenger.start()

            for (i in itemsRange) {
                Thread.sleep(Random().nextInt(100).toLong())
                lock.lock()
                while (bufferCount == MAXCOUNT) {
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

        val consumerBinaryMessenger = ZeroMqBinaryMessenger(nodes[1], nodes - nodes[1])
        val consumerMessenger = Messenger(consumerBinaryMessenger)
        val consumer = thread(start = true) {
            val lock = DistributedLock("distributedLock", consumerMessenger, ReentrantLock(), TimeManager(nodes - nodes[1]))
            val full = lock.newCondition("full")
            val empty = lock.newCondition("empty")
            consumerMessenger.start()

            for (i in itemsRange) {
                Thread.sleep(Random().nextInt(100).toLong())
                lock.lock()
                while (bufferCount == 0) {
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
        producerMessenger.close()
        consumerMessenger.close()

        assertEquals(itemsRange.toList(), result)
    }
}