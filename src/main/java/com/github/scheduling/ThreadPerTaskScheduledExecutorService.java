package com.github.scheduling;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.function.Consumer;

/**
 * {@link ScheduledExecutorService} implementation, adopted to virtual threads.
 * Unlike {@link java.util.concurrent.ScheduledThreadPoolExecutor ScheduledThreadPoolExecutor},
 * threads are not pooled and reused, but created per task by the means of 
 * {@link java.util.concurrent.ThreadPerTaskExecutor ThreadPerTaskExecutor}. 
 * Threads are removed from the thread list upon their completion.
 * 
 * <p/> While {@link #ThreadPerTaskScheduledExecutorService(ExecutorService)} constructor accepts arbitrary 
 * implementation of {@link ExecutorService}, only non-pooling, thread-per-task implementations 
 * will make sense because otherwise 
 * {@link java.util.concurrent.ScheduledThreadPoolExecutor ScheduledThreadPoolExecutor} could be used instead.
 * 
 * <p> <strong>Implementation details:</strong>
 * <ul>
 * <li>task submission is delegated to {@link java.util.concurrent.ThreadPerTaskExecutor ThreadPerTaskExecutor}
 * implementation of {@link ExecutorService};</li> 
 * <li>scheduling is organized by the means of stock {@link DelayQueue};</li>
 * <li>{@link Runnable} task is wrapped into a specialized subclass of {@link FutureTask}
 * which implements {@link Delayed} interface; 
 * almost all functionality, related to completion, exception/failure, cancellation etc 
 * is delegated to the base class; upon submission an instance of this class is placed into the queue;</li>
 * <li>internal queue reading tasks are submitted to the inner {@link ExecutorService} upon construction, 
 * these tasks {@link DelayQueue#take() take} the tasks instances from the queue and submit them to inner {@link ExecutorService},
 * these internal tasks get killed by means of {@link Thread#interrupt()} upon Service's shutdown or closure;</li>
 * <li> periodic tasks re-enqueue themselves upon completion, {@link FutureTask#runAndReset} is used 
 * to execute them instead of {@link FutureTask#run}.</li>
 * </ul>
 *
 */
public class ThreadPerTaskScheduledExecutorService implements ScheduledExecutorService {
	
	/** 
	 * Synchronizer provides concurrent access to a collection of threads and 
	 * atomic shutdown. Upon the shutdown via {@link #shutdown()} method,
	 * each registered thread is interrupted.
	 * These two tasks are not independent: first, the shutdown process excludes 
	 * simultaneous access to the collection of threads so that 
	 * no thread is allowed to be added when shutdown is in progress and, second, 
	 * only one thread is allowed to interrupt the registered threads.
	 * 
	 * <h5>Implementation details</h5>
	 * 
	 * <p/>In a very unlikely case of shutdown flag set by other thread between 
	 * the calls of {@link #isShutdown()} in {@link #shutdown()}  
	 * and {@link #tryAcquireOnShutdown} methods this thread will be,
	 * technically unnecessarily, suspended until that other thread completes 
	 * the shutdown process. Right after receiving a control, this thread will immediately 
	 * return without execution of shutdown action.
	 * 
	 * <p/>Similar strategy applied to a case of thread collection manipulation:
	 * a thread, which attempts to add a thread to thread collections
	 * will be suspended if shutdown is in progress. The suspension is not necessary
	 * because if shutdown is started, no thread registration is allowed and 
	 * {@link #registerThread(Thread)} method should return {@code false} 
	 * as soon as possible.
	 * 
	 * <p/>The both cases, which cause only insignificant delay, could happen upon 
	 * quite unlikely thread interleaving.  
	 * 
	 * <p/>{@link AbstractQueuedSynchronizer}'s {@code state} field is not used due to limitations
	 * of {@link AbstractQueuedSynchronizer#compareAndSetState} method, which does not allow
	 * to get a <i>witness</i> state. Instead {@link AtomicInteger} is used for {@link #state} field, which,
	 * it seems, does not have any adverse effect, but still increases the size of occupied memory. 
	 * Thus, effectively, only <i>waitset</i> feature of {@link AbstractQueuedSynchronizer} is used.       
	 */
	private static class SchedulerSync extends AbstractQueuedSynchronizer {
		
		private static final long serialVersionUID = -4132802382847403450L;

		// Actions

		/**
		 * A thread is about to be added to {@link #threads} collection.
		 */
		private static final int ADD_THREAD_ACTION = 1;

		/**
		 * A thread is about to be removed from {@link #threads} collection.
		 */
		private static final int REMOVE_THREAD_ACTION = 2;
		
		/**
		 * Shutdown is about to be performed.
		 */
		private static final int SHUTDOWN_ACTION = 3;
		
		
		// State flags
		
		/**
		 * {@link #threads} collection is locked to avoid 
		 * {@link ConcurrentModificationException}.
		 */
		private static final int THREADS_LOCK_FLAG = 1;
		
		/**
		 * Shutdown has been initiated. Whether it has been completed or not,
		 * depends on {@link #SHUTDOWN_IN_PROGRESS_FLAG} flag.
		 */
		private static final int SHUTDOWN_FLAG = 2;

		/**
		 * Shutdown has been initiated and is currently in progress.
		 */
		private static final int SHUTDOWN_IN_PROGRESS_FLAG = 4;
		
		/**
		 * Denotes a state when no any of above flags is set.
		 */
		private static final int EMPTY_STATE = 0;
		
		//
		
		/**
		 * Collection of delayed threads. 
		 * Guarded by the {@link AbstractQueuedSynchronizer}'s 
		 * acquisition/release framework.
		 */
		private final Set<Thread> threads = new HashSet<>(); // 
		
		/**
		 * Overrides the superclass' field, accessible via {@link #setState(int)} and 
		 * {@link #getState()} methods due to a necessity of employing 
		 * {@link AtomicInteger}'s compare-and-exchange logic.  
		 */
		private final AtomicInteger state = new AtomicInteger();
	    
		@Override
		protected boolean tryAcquire(int action) {
			if (action == ADD_THREAD_ACTION)
				return tryAcquireOnThreadAddition();
			else if (action == REMOVE_THREAD_ACTION)
				return tryAcquireOnThreadRemoval();
			else if (action == SHUTDOWN_ACTION)
				return tryAcquireOnShutdown();
			else 
				throw new Error("Unexpected action " + action);
		}
		
		/**
		 * Acquires if current state is empty, 
		 * {@link #THREADS_LOCK_FLAG} was set in this case, 
		 * <b>or</b> shutdown has been completed.
		 * A caller is supposed to check result of {@link #isShutdown()} 
		 * to differentiate between these two cases. 
		 */
		private boolean tryAcquireOnThreadAddition() {
			if (state.compareAndSet(EMPTY_STATE, THREADS_LOCK_FLAG))
				return true;
			else if (isShutdown())
				return true;
			else
				return false;
		}
		
		/**
		 * Acquires if current state is empty <b>or</b> 
		 * shutdown has been completed, {@link #THREADS_LOCK_FLAG} was set in these cases.
		 */
		private boolean tryAcquireOnThreadRemoval() {
			final int expected = isShutdown() ? SHUTDOWN_FLAG : EMPTY_STATE;
			if (state.compareAndSet(expected, expected | THREADS_LOCK_FLAG))
				return true;
			else 
				return false;
		}
		
		/**
		 * Acquires if current state is empty, flags {@link #THREADS_LOCK_FLAG},
		 * {@link #SHUTDOWN_FLAG}, and {@link #SHUTDOWN_IN_PROGRESS_FLAG} are set in this case,
		 * <b>or</b> shutdown has been fully completed. A caller is supposed to check 
		 * results of {@link #isShutdown()} and {@link #isShutdown()}
		 * to differentiate between these two cases. 
		 */
		private boolean tryAcquireOnShutdown() {
			final int witnessState = state.compareAndExchange(EMPTY_STATE, 
					THREADS_LOCK_FLAG | SHUTDOWN_FLAG | SHUTDOWN_IN_PROGRESS_FLAG);
			if (witnessState == 0 || 
					(isShutdown(witnessState) && !isShutdownInProgress(witnessState)))
				return true;
			else
				return false;
		}
		
		@Override
		protected boolean tryRelease(int action) {
			if (action == ADD_THREAD_ACTION || action == REMOVE_THREAD_ACTION)
				return tryReleaseOnThreadAddingOrRemoval();
			else if (action == SHUTDOWN_ACTION)
				return tryReleaseOnShutdown();
			else 
				throw new Error("Unexpected action " + action);
		}
		
		/**
		 * Removes {@link #THREADS_LOCK_FLAG} flag. 
		 * Checks if it was set, and if it was not,
		 * throws {@link IllegalStateException}.
		 */
		private boolean tryReleaseOnThreadAddingOrRemoval() {
			final int state = this.state.get();
			final int newState = state & ~THREADS_LOCK_FLAG;
			if (!this.state.compareAndSet(state, newState))
				throw new IllegalStateException("Lock hasn't been acquired " + state + " prior to the release");
			return true;
		}
		
		/**
		 * Removes {@link #THREADS_LOCK_FLAG} and {@link #SHUTDOWN_IN_PROGRESS_FLAG} flags. 
		 * Checks if they were set, and if they were not,
		 * throws {@link IllegalStateException}.
		 */
		private boolean tryReleaseOnShutdown() {
			final int state = this.state.get();
			final int newState = state & ~(THREADS_LOCK_FLAG | SHUTDOWN_IN_PROGRESS_FLAG);
			if (!this.state.compareAndSet(state, newState))
				throw new IllegalStateException("Lock hasn't been acquired " + state + " prior to the release");
			return true;
		}
		
		public boolean isShutdown() {
			return isShutdown(state.get());
		}

		private boolean isShutdownInProgress() {
			return isShutdownInProgress(state.get());
		}
		
		private boolean isShutdown(int state) {
			return (state & SHUTDOWN_FLAG) == SHUTDOWN_FLAG;
		}

		private boolean isShutdownInProgress(int state) {
			return (state & SHUTDOWN_IN_PROGRESS_FLAG) == SHUTDOWN_IN_PROGRESS_FLAG;
		}
		
		/**
		 * Attempts to add Thread {@code t} to the collection of threads.
		 *  
		 * @return {@code true} if the attempt is successful; 
		 * {@code false} - if shutdown procedure has been already completed 
		 * and {@link #isShutdown()} return {@code true}; in this case 
		 * the caller is supposed to skip the actions it intended to do
		 * and the call to {@link #removeThread(Thread)} is unnecessary.
		 */
		public boolean registerThread(Thread t) {
			if (!isShutdown()) {
				super.acquire(ADD_THREAD_ACTION);
				try {
					if (!isShutdown()) {
						threads.add(t);
						return true;
					} else 
						return false;
				} finally {
					super.release(ADD_THREAD_ACTION);
				}
			} else
				return false;
		}
		
		/**
		 * Removes a Thread {@code t} from the collection of threads.
		 *  
		 * @return {@code true} if the attempt is successful; 
		 * {@code false} - if shutdown procedure has been already completed 
		 * and {@link #isShutdown()} return {@code true}; in this case 
		 * the caller is supposed to skip the actions it intended to do
		 * and the call to {@link #removeThread(Thread)} is unnecessary.
		 */
		public void deregisterThread(Thread t) {
			super.acquire(REMOVE_THREAD_ACTION);
			try {
				threads.remove(t);
			} finally {
				super.release(REMOVE_THREAD_ACTION);
			}
		}
		
		/**
		 * Shuts down Delayed Threads, calling {@link Thread#interrupt()}.
		 * Does nothing if the shutdown procedure has been already completed 
		 * and {@link #isShutdown()} return {@code true}.
		 */
		public void shutdown() {
			if (!isShutdown()) {
				super.acquire(SHUTDOWN_ACTION);
				try {
					if (isShutdown() && isShutdownInProgress()) {
						threads.stream().forEach(t -> t.interrupt());
					}
				} finally {
					super.release(SHUTDOWN_ACTION);
				}
			}
		}
		
	}
	
	private final ExecutorService executorService;
	private final DelayQueue<DelayedTask<?>> queue = new DelayQueue<>();
	private final SchedulerSync schedulerSync = new SchedulerSync();
	
	public ThreadPerTaskScheduledExecutorService() {
		this(Executors.newVirtualThreadPerTaskExecutor());
	}
	
	public ThreadPerTaskScheduledExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}
	
	// ExecutorService

	public void execute(Runnable command) {
		executorService.execute(command);
	}

	public void shutdown() {
		shutdownScheduler();
		executorService.shutdown();
	}

	public List<Runnable> shutdownNow() { 
		shutdownScheduler();
		return executorService.shutdownNow();
	}

	/**
	 * Multi-threaded access to this method is allowed because 
	 * {@link #queue} allows it and {@link #schedulerSync} synchronizes 
	 * its {@link SchedulerSync#shutdown()} method.
	 */
	private void shutdownScheduler() {
		schedulerSync.shutdown();
		DelayedTask<?> task = null;
		while ((task = queue.peek()) != null) {
			queue.remove(task);
			task.cancel(true);
		}
	}

	public boolean isShutdown() {
		return executorService.isShutdown();
	}

	public boolean isTerminated() {
		return executorService.isTerminated();
	}

	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return executorService.awaitTermination(timeout, unit);
	}

	public <T> Future<T> submit(Callable<T> task) {
		return executorService.submit(task);
	}

	public <T> Future<T> submit(Runnable task, T result) {
		return executorService.submit(task, result);
	}

	public Future<?> submit(Runnable task) {
		return executorService.submit(task);
	}

	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return executorService.invokeAll(tasks);
	}

	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		return executorService.invokeAll(tasks, timeout, unit);
	}

	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return executorService.invokeAny(tasks);
	}

	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return executorService.invokeAny(tasks, timeout, unit);
	}

	public void close() {
		ScheduledExecutorService.super.close();
		executorService.close();
	}
	
	// ScheduledExecutorService
	
	private static class DelayedTask<V> extends FutureTask<V> implements ScheduledFuture<V> {
		
		public enum PeriodicMode {
			RATE,
			DELAY,
			NONE
		}
		
		private final long period;
		private final PeriodicMode periodicMode;
		private final Consumer<DelayedTask<V>> enqueuer;
		private volatile long triggerTime;
		
		public DelayedTask(Callable<V> callable, long triggerTime, Consumer<DelayedTask<V>> enqueuer) {
			this(callable, triggerTime, -1, PeriodicMode.NONE, enqueuer);
		}

		public DelayedTask(Callable<V> callable, long triggerTime, long period, PeriodicMode periodicMode, Consumer<DelayedTask<V>> enqueuer) {
			super(callable);
			this.period = period;
			this.periodicMode = periodicMode;
			this.triggerTime = triggerTime;
			this.enqueuer = enqueuer;
			enqueuer.accept(this);
		}
		
		@Override
		public int compareTo(Delayed o) {
			return o instanceof DelayedTask other ? Long.signum(triggerTime - other.triggerTime) : 1;
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(triggerTime - System.nanoTime(), NANOSECONDS);
		}
		
		@Override
		public void run() {
			switch (periodicMode) {
				case NONE -> {
					super.run();
				}
				case DELAY -> {
					runAndReset();
					triggerTime = System.nanoTime() + period;
					enqueuer.accept(this);
				}
				case RATE -> {
					final long nextRun = System.nanoTime() + period;
					runAndReset();
					triggerTime = Math.max(nextRun, System.nanoTime());
					enqueuer.accept(this);
				}
				default -> {
					throw new Error("Unexpected " + PeriodicMode.class.getSimpleName() + " value " + periodicMode);
				}
			}
		}
		
	}
	
	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
		return schedule(() -> {
			command.run();
			return null;
		}, delay, unit);
	}

	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		return new DelayedTask<>(callable, triggerTime(delay, unit), this::enqueueTask);
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
		return new DelayedTask<>(() -> {
			command.run();
			return null;
		}, triggerTime(initialDelay, unit), unit.toNanos(period), DelayedTask.PeriodicMode.RATE, this::enqueueTask);
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
		return new DelayedTask<>(() -> {
			command.run();
			return null;
		}, triggerTime(initialDelay, unit), unit.toNanos(delay), DelayedTask.PeriodicMode.DELAY, this::enqueueTask);
	}
	
	private void enqueueTask(DelayedTask<?> task) {
		if (schedulerSync.isShutdown())
			throw new RejectedExecutionException("Service is shut down"); 
		queue.add(task);
		executorService.submit(() -> {
			while (true) {
				boolean delayedThreadAdded = false;
				try {
					delayedThreadAdded = schedulerSync.registerThread(Thread.currentThread());
					if (delayedThreadAdded) {
						final DelayedTask<?> readyTask = queue.take();
						readyTask.run();
					} 
					break;
				} catch (InterruptedException e) {
					// ignore the interruptions, not caused by shutdown
					if (schedulerSync.isShutdown())
						break;
				} finally {
					if (delayedThreadAdded)
						schedulerSync.deregisterThread(Thread.currentThread());					
				}
			}
		});
		
	}
	
	// Adopted from ScheduledThreadPoolExecutor
	
    /**
     * Returns the nanoTime-based trigger time of a delayed action.
     */
    private long triggerTime(long delay, TimeUnit unit) {
        return triggerTime(unit.toNanos((delay < 0) ? 0 : delay));
    }
    /**
     * Returns the nanoTime-based trigger time of a delayed action.
     */
    private long triggerTime(long delay) {
        return System.nanoTime() +
            ((delay < (Long.MAX_VALUE >> 1)) ? delay : overflowFree(delay));
    }
    
    /**
     * Constrains the values of all delays in the queue to be within
     * Long.MAX_VALUE of each other, to avoid overflow in compareTo.
     * This may occur if a task is eligible to be dequeued, but has
     * not yet been, while some other task is added with a delay of
     * Long.MAX_VALUE.
     */
    private long overflowFree(long delay) {
        Delayed head = (Delayed) queue.peek();
        if (head != null) {
            long headDelay = head.getDelay(NANOSECONDS);
            if (headDelay < 0 && (delay - headDelay < 0))
                delay = Long.MAX_VALUE + headDelay;
        }
        return delay;
    }
    
}
