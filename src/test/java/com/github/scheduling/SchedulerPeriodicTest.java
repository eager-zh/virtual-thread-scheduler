package com.github.scheduling;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

public class SchedulerPeriodicTest {
	
	private static boolean sleep(long millis) {
		try {
			Thread.sleep(millis);
			return true;
		} catch (InterruptedException e) {
			return false;
		}
	}
	
	private ScheduledExecutorService getScheduler() {
		return new ThreadPerTaskScheduledExecutorService();
		// may want to compare with 
		// Executors.newScheduledThreadPool(Integer.MAX_VALUE, Thread.ofVirtual().factory());
		// in order to make sure that ScheduledExecutorService contract is fulfilled
	}
	
	// Success
	
	@Test
	public void testFixedRateSucceeded() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		final AtomicInteger executionCount = new AtomicInteger();
		final AtomicBoolean futureReturnedFromGet = new AtomicBoolean();
		
		final Future<?> future = service.scheduleAtFixedRate( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			executionCount.incrementAndGet();
			sleep(1000);
		}, 0, 5, TimeUnit.SECONDS);
		
		Thread.ofVirtual().start( () -> {
			try {
				future.get();
				futureReturnedFromGet.set(true);
			} catch (CancellationException e) {
			} catch (Exception e) {
				Assert.fail("Task threw exception " + e);
			}
		});
		
		sleep(12_000); 

		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(5, TimeUnit.SECONDS));

		Assert.assertEquals("Future is not executed expected amount of times", 3, executionCount.get());
		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertTrue("Future must be cancelled", future.isCancelled());
		Assert.assertFalse("Future should not return from get method", futureReturnedFromGet.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in cancelled state", Future.State.CANCELLED, future.state());
	}
	
	@Test
	public void testFixedDelaySucceeded() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		final AtomicInteger executionCount = new AtomicInteger();
		final AtomicBoolean futureReturnedFromGet = new AtomicBoolean();
		final AtomicReference<Exception> exception = new AtomicReference<>();
		
		final Future<?> future = service.scheduleWithFixedDelay( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			executionCount.incrementAndGet();
			sleep(1000);
		}, 0, 3, TimeUnit.SECONDS);
		
		Thread.ofVirtual().start( () -> {
			try {
				future.get();
				futureReturnedFromGet.set(true);
			} catch (InterruptedException | ExecutionException e) {
				exception.set(e);
			}
		});
		
		sleep(10_000);

		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));

		Assert.assertEquals("Future is not executed expected amount of times", 3, executionCount.get());
		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertTrue("Future must be cancelled", future.isCancelled());
		Assert.assertFalse("Future should not return from get method", futureReturnedFromGet.get());
		Assert.assertEquals("Future's get method should not throw exception", null,
				exception.get() == null ? null : exception.get().getClass());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in cancelled state", Future.State.CANCELLED, future.state());
	}
	
	// Failure

	@Test
	public void testFixedRateFailed() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		
		final Future<?> future = service.scheduleAtFixedRate( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			sleep(1000);
			throw new RuntimeException("Task failed");
		}, 0, 5, TimeUnit.SECONDS);
		
		try {
			future.get();
			Assert.fail("ExecutionException should be thrown");
		} catch (ExecutionException e) {
			Assert.assertEquals("RuntimeException should be a cause of failure", RuntimeException.class,
					e.getCause().getClass());
			Assert.assertEquals("Exception message should be \"Task failed\"", "Task failed",
					e.getCause().getMessage());
		}

		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in failed state", Future.State.FAILED, future.state());
		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}

	@Test
	public void testFixedDelayFailed() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		
		final Future<?> future = service.scheduleWithFixedDelay( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			sleep(1000);
			throw new RuntimeException("Task failed");
		}, 0, 5, TimeUnit.SECONDS);
		
		try {
			future.get();
			Assert.fail("ExecutionException should be thrown");
		} catch (ExecutionException e) {
			Assert.assertEquals("RuntimeException should be a cause of failure", RuntimeException.class,
					e.getCause().getClass());
			Assert.assertEquals("Exception message should be \"Task failed\"", "Task failed",
					e.getCause().getMessage());
		}

		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in failed state", Future.State.FAILED, future.state());
		Assert.assertFalse("Future must not be cancelled", future.isCancelled());
		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}
	
	// Cancellation
	
	@Test
	public void testFixedRateCancelled() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		
		final Future<?> future = service.scheduleAtFixedRate( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			sleep(10_000);
		}, 0, 5, TimeUnit.SECONDS);
		
		Thread.ofVirtual().start(() -> {
			sleep(3_000);
			future.cancel(true);
		});
		
		try {
			future.get();
			Assert.fail("CancellationException must be thrown");
		} catch (CancellationException e) {
		}
		
		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in cancelled state", Future.State.CANCELLED, future.state());
		Assert.assertTrue("Future must be cancelled", future.isCancelled());
		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}

	@Test
	public void testFixedDelayCancelled() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		
		final Future<?> future = service.scheduleWithFixedDelay( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			sleep(10_000);
		}, 0, 5, TimeUnit.SECONDS);
		
		Thread.ofVirtual().start(() -> {
			sleep(3_000);
			future.cancel(true);
		});
		
		try {
			future.get();
			Assert.fail("CancellationException must be thrown");
		} catch (CancellationException e) {
		}
		
		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in cancelled state", Future.State.CANCELLED, future.state());
		Assert.assertTrue("Future must be cancelled", future.isCancelled());
		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}
	
	@Test
	public void testFixedRateCancelledUnstarted() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		
		final Future<?> future = service.scheduleAtFixedRate( ()->  {
			sleep(10_000);
		}, 10, 5, TimeUnit.SECONDS);
		
		Thread.ofVirtual().start(() -> {
			sleep(3_000);
			future.cancel(true);
		});
		
		try {
			future.get();
			Assert.fail("CancellationException must be thrown");
		} catch (CancellationException e) {
		}
		
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in cancelled state", Future.State.CANCELLED, future.state());
		Assert.assertTrue("Future must be cancelled", future.isCancelled());
		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}
	
	@Test
	public void testFixedDelayCancelledUnstarted() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		
		final Future<?> future = service.scheduleWithFixedDelay( ()->  {
			sleep(10_000);
		}, 10, 5, TimeUnit.SECONDS);
		
		Thread.ofVirtual().start(() -> {
			sleep(3_000);
			future.cancel(true);
		});
		
		try {
			future.get();
			Assert.fail("CancellationException must be thrown");
		} catch (CancellationException e) {
		}
		
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in cancelled state", Future.State.CANCELLED, future.state());
		Assert.assertTrue("Future must be cancelled", future.isCancelled());
		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}
	
	// Shutdown and closure

	@Test
	public void testShutdown() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		
		service.shutdown();
		
		try {
			service.scheduleAtFixedRate( ()->  {
			}, 0, 3, TimeUnit.SECONDS);
			Assert.fail("Task should not be scheduled successfully if the Scheduled service is shut down");
		} catch (RejectedExecutionException e) {
		}

		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(5, TimeUnit.SECONDS));
	}
	
	@Test
	public void testMultiShutdown() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		final AtomicInteger executionCount = new AtomicInteger();
		final AtomicBoolean futureReturnedFromGet = new AtomicBoolean();
		
		final Future<?> future = service.scheduleAtFixedRate( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			executionCount.incrementAndGet();
			sleep(1000);
		}, 0, 5, TimeUnit.SECONDS);
		
		Thread.ofVirtual().start( () -> {
			try {
				future.get();
				futureReturnedFromGet.set(true);
			} catch (CancellationException e) {
			} catch (Exception e) {
				Assert.fail("Task threw exception " + e);
			}
		});
		
		sleep(12_000); 

		for (int i =0; i < 3; i++ )
			Thread.ofVirtual().start( () -> {
				service.shutdown();
			});

		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(5, TimeUnit.SECONDS));

		Assert.assertEquals("Future is not executed expected amount of times", 3, executionCount.get());
		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertTrue("Future must be cancelled", future.isCancelled());
		Assert.assertFalse("Future should not return from get method", futureReturnedFromGet.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in cancelled state", Future.State.CANCELLED, future.state());
	}
	
	@Test
	public void testClose() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		final AtomicInteger executionCount = new AtomicInteger();
		final AtomicBoolean futureReturnedFromGet = new AtomicBoolean();
		
		final Future<?> future = service.scheduleAtFixedRate( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			executionCount.incrementAndGet();
			sleep(1000);
		}, 0, 5, TimeUnit.SECONDS);
		
		Thread.ofVirtual().start( () -> {
			try {
				future.get();
				futureReturnedFromGet.set(true);
			} catch (CancellationException e) {
			} catch (Exception e) {
				Assert.fail("Task threw exception " + e);
			}
		});
		
		sleep(12_000); 

		service.close();
		Assert.assertTrue("Scheduled service must terminate", service.isTerminated());

		Assert.assertEquals("Future is not executed expected amount of times", 3, executionCount.get());
		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertTrue("Future must be cancelled", future.isCancelled());
		Assert.assertFalse("Future should not return from get method", futureReturnedFromGet.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in cancelled state", Future.State.CANCELLED, future.state());
	}
	
	@Test 
	public void testFixedRateSucceededMultiple() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicInteger executionCount = new AtomicInteger();
		
		final List<ScheduledFuture<?>> futures = IntStream.range(0, 4)
				.mapToObj((i) -> service.scheduleAtFixedRate( ()->  {
					executionCount.incrementAndGet();
					sleep(1000);
				}, 0, 5, TimeUnit.SECONDS))
				.collect(Collectors.toList());
		
		sleep(13_000); 

		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(5, TimeUnit.SECONDS));

		Assert.assertEquals("Futures is not executed expected amount of times", 12, executionCount.get());
		
		futures.forEach(future -> {
			Assert.assertTrue("Future must be in cancelled state", future.isCancelled());
		});
	}

}
