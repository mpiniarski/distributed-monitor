package pl.mpiniarski.distributedmonitor

import pl.mpiniarski.distributedmonitor.communication.ZeroMqBinaryMessenger

open class DistributedMonitor {
    protected val lock : DistributedLock = DistributedLock("", listOf(""), ZeroMqBinaryMessenger("", listOf("")))

    protected fun createCondition() : DistributedCondition {
        return lock.newCondition()
    }

    protected inline fun <T> entry(f : () -> T) : T {
        try {
            lock.lock()
            return f()
        } finally {
            lock.unlock()
        }
    }
}

class DistributedCondition {
    fun await() {
    }

    fun signal() {
    }
}
