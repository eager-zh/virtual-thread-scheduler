# Scheduled Executor Service for virtual threads

The repository offers an implementation of [ScheduledExecutorService](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ScheduledExecutorService.html), `ThreadPerTaskScheduledExecutorService` class, suitable for virtual threads and inspired by a discussion in a Stack Overflow thread [How to use virtual threads with ScheduledExecutorService](https://stackoverflow.com/questions/76587253/how-to-use-virtual-threads-with-scheduledexecutorservice).

As it follows from the discussion, none of the solutions, offered in the answers, is able to satisfy recommendations for [non-pooling](https://docs.oracle.com/en/java/javase/21/core/virtual-threads.html#GUID-C0FEE349-D998-4C9D-B032-E01D06BE55F2) of virtual threads and [one-virtual-thread-per-task strategy](https://docs.oracle.com/en/java/javase/21/core/virtual-threads.html#GUID-B163BC51-039B-43BD-87ED-27BE384B509D) at the same time fulfilling the contract of `ScheduledExecutorService`. At the time of this writing, no such off-the-shelf solution is provided by the Java Concurrency classes either.

By default, the solution is based upon `java.util.concurrent.ThreadPerTaskExecutor` implementation of [ExecutorService](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ExecutorService.html) interface, accessible via  [Executors.newVirtualThreadPerTaskExecutor](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Executors.html#newVirtualThreadPerTaskExecutor()) methods, available since version 21. 

Java version 21 or later is required. 
