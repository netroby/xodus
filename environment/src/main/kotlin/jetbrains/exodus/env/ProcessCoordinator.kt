package jetbrains.exodus.env

interface ProcessCoordinator : AutoCloseable {
    var highestRoot: Long?
    val lowestUsedRoot: Long?
    var localLowestUsedRoot: Long?

    fun tryAcquireWriterLock(): Boolean

    fun <T> withHighestRootLock(action: () -> T): T

    override fun close()
}
