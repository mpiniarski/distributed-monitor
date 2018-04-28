package pl.mpiniarski.distributedmonitor

import junit.framework.Assert.assertEquals
import junit.framework.Assert.assertTrue
import org.junit.Test
import pl.mpiniarski.distributedmonitor.communication.ZeroMqBinaryMessenger
import java.util.*
import kotlin.concurrent.thread

class DistributedLockTest {


    @Test
    fun lockOn2Hosts() {
        val nodes = listOf(
                "tcp://localhost:5557",
                "tcp://localhost:5558"
        )
        var count = 0
        val communicator1 = ZeroMqBinaryMessenger(nodes[0], nodes - nodes[0])
        val lock1 = DistributedLock(nodes[0], nodes - nodes[0], communicator1)
        val communicator2 = ZeroMqBinaryMessenger(nodes[1], nodes - nodes[1])
        val lock2 = DistributedLock(nodes[1], nodes - nodes[1], communicator2)

        val thread1 = thread(start = true) {
            lock1.lock()
            System.out.println("Thread 1 enter")
            val myCount = count
            Thread.sleep(1000)
            count = myCount + 1
            lock1.unlock()
            System.out.println("Thread 1 leave")
        }

        val thread2 = thread(start = true) {
            lock2.lock()
            System.out.println("Thread 2 enter")
            val myCount = count
            Thread.sleep(1000)
            count = myCount + 1
            lock2.unlock()
            System.out.println("Thread 2 leave")
        }

        thread1.join()
        thread2.join()

        assertTrue(count == 2)
        communicator1.close()
        communicator2.close()
    }

    @Test
    fun lockOn10Hosts() {
        val nodes = listOf(
                "tcp://localhost:5557",
                "tcp://localhost:5558",
                "tcp://localhost:5559",
                "tcp://localhost:5560",
                "tcp://localhost:5561",
                "tcp://localhost:5562",
                "tcp://localhost:5563",
                "tcp://localhost:5564",
                "tcp://localhost:5565",
                "tcp://localhost:5566"
        )

        var count = 0

        (0 .. 9).map {
            val communicator = ZeroMqBinaryMessenger(nodes[it], nodes - nodes[it])
            val lock = DistributedLock(nodes[it], nodes - nodes[it], communicator)
            thread(start = true) {
                Thread.sleep(Random().nextInt(100).toLong())
                lock.lock()
                System.out.println("Thread $it enter")
                val myCount = count
                Thread.sleep(100)
                count = myCount + 1
                lock.unlock()
                System.out.println("Thread $it leave")
            }
        }.forEach {
            it.join()
        }

        assertEquals(10, count)
    }
}