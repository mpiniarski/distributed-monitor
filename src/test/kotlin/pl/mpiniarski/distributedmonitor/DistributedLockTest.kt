package pl.mpiniarski.distributedmonitor

import junit.framework.Assert.assertEquals
import junit.framework.Assert.assertTrue
import org.junit.Test
import pl.mpiniarski.distributedmonitor.communication.Messenger
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
        val binaryMessenger1 = ZeroMqBinaryMessenger(nodes[0], nodes - nodes[0])
        val messenger1 = Messenger(binaryMessenger1)
        val lock1 = DistributedLock("lock", messenger1)
        messenger1.start()
        val binaryMessenger2 = ZeroMqBinaryMessenger(nodes[1], nodes - nodes[1])
        val messenger2 = Messenger(binaryMessenger2)
        val lock2 = DistributedLock("lock", messenger2)
        messenger2.start()

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
        binaryMessenger1.close()
        binaryMessenger2.close()
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
            val binaryMessenger = ZeroMqBinaryMessenger(nodes[it], nodes - nodes[it])
            val messenger = Messenger(binaryMessenger)
            val lock = DistributedLock("lock", messenger)
            messenger.start()
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