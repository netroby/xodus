package jetbrains.exodus.env

class DummyProcessCoordinator : ProcessCoordinator {
    @Volatile override var highestRoot: Long? = null
    override val lowestUsedRoot: Long? get() = localLowestUsedRoot
    @Volatile override var localLowestUsedRoot: Long? = null

    override fun tryAcquireWriterLock() = true

    override fun <T> withHighestRootLock(action: () -> T) = action()

    override fun close() {}
}
