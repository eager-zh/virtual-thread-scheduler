package com.github.scheduling;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

public class SchedulerTest {
	
	private ScheduledExecutorService getScheduler() {
		return
			new ThreadPerTaskScheduledExecutorService();
		// may want to compare with 
		// Executors.newScheduledThreadPool(Integer.MAX_VALUE, Thread.ofVirtual().factory());
		// in order to make sure that ScheduledExecutorService contract is fulfilled
	}
	
	private static boolean sleep(long millis) {
		try {
			Thread.sleep(millis);
			return true;
		} catch (InterruptedException e) {
			return false;
		}
	}
	
	// Success
	
	@Test
	public void testSucceededImmediate() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		
		final Future<?> future = service.schedule( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			sleep(1000);
		}, 0, TimeUnit.SECONDS);
		
		future.get();
		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in succeeded state", Future.State.SUCCESS, future.state());
		Assert.assertFalse("Future must not be cancelled", future.isCancelled());
		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}

	@Test
	public void testSucceededDelayed() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		
		final Future<?> future = service.schedule( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			sleep(1000);
		}, 5, TimeUnit.SECONDS);
		
		future.get();
		
		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in succeeded state", Future.State.SUCCESS, future.state());
		Assert.assertFalse("Future must not be cancelled", future.isCancelled());
		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}
	
	@Test
	public void testSucceededCallable() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		
		final Future<String> future = service.schedule( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			sleep(1000);
			return "OK";
		}, 5, TimeUnit.SECONDS);
		
		Assert.assertEquals("Future should return OK result", "OK", future.get());
		
		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in succeeded state", Future.State.SUCCESS, future.state());
		Assert.assertFalse("Future must not be cancelled", future.isCancelled());
		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}
	
	@Test 
	public void testSucceededMultiple() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicInteger executionCount = new AtomicInteger();
		
		final List<ScheduledFuture<?>> futures = IntStream.range(0, 4)
				.mapToObj((i) -> service.schedule( ()->  {
						executionCount.incrementAndGet();
						sleep(1000);
					}, i*2, TimeUnit.SECONDS))
				.collect(Collectors.toList());
		
		sleep(12_000); 

		Assert.assertEquals("Futures is not executed expected amount of times", 4, executionCount.get());

		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(5, TimeUnit.SECONDS));
		
		futures.forEach(future -> {
			Assert.assertTrue("Future must be done", future.isDone());
			Assert.assertEquals("Future must be in succeeded state", Future.State.SUCCESS, future.state());
			Assert.assertFalse("Future must not be cancelled", future.isCancelled());
		});
	}
	
	// Cancellation

	@Test
	public void testCancelledUnstarted() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean wasTaskActive = new AtomicBoolean();
		
		final Future<?> future = service.schedule( ()->  {
			wasTaskActive.set(true);
			sleep(1000);
		}, 10, TimeUnit.SECONDS);
		
		sleep(5000);
		Assert.assertTrue("Task must be successfully cancelled", future.cancel(true));
		
		Assert.assertFalse("Task must not be active", wasTaskActive.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in cancelled state", Future.State.CANCELLED, future.state());
		Assert.assertTrue("Future must be cancelled", future.isCancelled());
		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}

	@Test
	public void testCancelledStarted() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		
		final Future<?> future = service.schedule( ()->  {
			isVirtual.set(true);
			sleep(20_000);
		}, 5, TimeUnit.SECONDS);
		
		sleep(10_000);
		Assert.assertTrue("Task must be successfully cancelled", future.cancel(true));

		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in cancelled state", Future.State.CANCELLED, future.state());
		Assert.assertTrue("Future must be cancelled", future.isCancelled());
		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}
	
	@Test
	public void testCancelledAsyncStarted() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		
		final Future<?> future = service.schedule( ()->  {
			isVirtual.set(true);
			sleep(20_000);
		}, 5, TimeUnit.SECONDS);
		
		Thread.ofVirtual().start(() -> {
			sleep(10_000);
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
	public void testCancelledAsyncUnstarted() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean wasActive = new AtomicBoolean();
		
		final Future<?> future = service.schedule( ()->  {
			wasActive.set(true);
			sleep(20_000);
		}, 10, TimeUnit.SECONDS);
		
		Thread.ofVirtual().start(() -> {
			sleep(5_000);
			future.cancel(true);
		});
		
		try {
			future.get();
			Assert.fail("CancellationException must be thrown");
		} catch (CancellationException e) {
		}
		
		Assert.assertFalse("Task must not be active", wasActive.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in cancelled state", Future.State.CANCELLED, future.state());
		Assert.assertTrue("Future must be cancelled", future.isCancelled());
		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}
	
	@Test
	public void testCancelledAsyncUnstartedTimed() throws InterruptedException, ExecutionException, TimeoutException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean wasActive = new AtomicBoolean();
		
		final Future<?> future = service.schedule( ()->  {
			wasActive.set(true);
			sleep(20_000);
		}, 10, TimeUnit.SECONDS);
		
		Thread.ofVirtual().start(() -> {
			sleep(5_000);
			future.cancel(true);
		});
		
		try {
			future.get(20, TimeUnit.SECONDS);
			Assert.fail("CancellationException must be thrown");
		} catch (CancellationException e) {
		}
		
		Assert.assertFalse("Task must not be active", wasActive.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in cancelled state", Future.State.CANCELLED, future.state());
		Assert.assertTrue("Future must be cancelled", future.isCancelled());
		service.shutdown();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}
	
	// Failure
	
	@Test
	public void testFailedBeforeWaiting() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		
		final Future<?> future = service.schedule( ()->  {
			isVirtual.set(true);
			sleep(5_000);
			throw new RuntimeException("Task failed");
		}, 5, TimeUnit.SECONDS);
		
		sleep(10_000);
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
	
	@Test
	public void testFailedOnWaiting() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		
		final Future<?> future = service.schedule( ()->  {
			isVirtual.set(true);
			sleep(20_000);
			throw new RuntimeException("Task failed");
		}, 10, TimeUnit.SECONDS);
		
		sleep(5_000);
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
	
	// Timeout

	@Test
	public void testTimeoutStarted() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		
		final Future<?> future = service.schedule( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			sleep(10000);
		}, 10, TimeUnit.SECONDS);
		
		try {
			future.get(15, TimeUnit.SECONDS);
			Assert.fail("TimeoutException should be thrown");
		} catch (TimeoutException e) {
		}
		
		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertFalse("Future must not be done", future.isDone());
		Assert.assertFalse("Future must not be cancelled", future.isCancelled());
		Assert.assertEquals("Future must be in running state", Future.State.RUNNING, future.state());
		service.shutdownNow();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}

	@Test
	public void testTimeoutUnstarted() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean wasActive = new AtomicBoolean();
		
		final Future<?> future = service.schedule( ()->  {
			wasActive.set(true);
			sleep(10000);
		}, 20, TimeUnit.SECONDS);
		
		try {
			future.get(10, TimeUnit.SECONDS);
			Assert.fail("TimeoutException should be thrown");
		} catch (TimeoutException e) {
		}
		
		Assert.assertFalse("Task must not be active", wasActive.get());
		Assert.assertFalse("Future must not be done", future.isDone());
		Assert.assertFalse("Future must not be cancelled", future.isCancelled());
		Assert.assertEquals("Future must be in running state", Future.State.RUNNING, future.state());
		service.shutdownNow();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}
	
	// Interruption
	
	@Test
	public void testInterruptedStarted() throws ExecutionException, InterruptedException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		final Thread thread = Thread.currentThread(); 
		
		final Future<?> future = service.schedule( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			sleep(10000);
		}, 0, TimeUnit.SECONDS);
		
		Thread.ofVirtual().start(() -> {
			sleep(5000);
			thread.interrupt();
		});
		
		try {
			future.get();
			Assert.fail("InterruptedException must be thrown");
		} catch (InterruptedException e) {
		}
		
		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertFalse("Future must not be in done state", future.isDone());
		Assert.assertEquals("Future must be in running state", Future.State.RUNNING, future.state());
		Assert.assertFalse("Future must not be cancelled", future.isCancelled());
		service.shutdownNow();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}

	@Test
	public void testInterruptedUnstarted() throws ExecutionException, InterruptedException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean wasActive = new AtomicBoolean();
		final Thread thread = Thread.currentThread(); 
		
		final Future<?> future = service.schedule( ()->  {
			wasActive.set(Thread.currentThread().isVirtual());
			sleep(10000);
		}, 20, TimeUnit.SECONDS);
		
		Thread.ofVirtual().start(() -> {
			sleep(5000);
			thread.interrupt();
		});
		
		try {
			future.get();
			Assert.fail("InterruptedException must be thrown");
		} catch (InterruptedException e) {
		}
		
		Assert.assertFalse("Task must not be active", wasActive.get());
		Assert.assertFalse("Future must not be done", future.isDone());
		Assert.assertEquals("Future must be in running state", Future.State.RUNNING, future.state());
		Assert.assertFalse("Future must not be cancelled", future.isCancelled());
		service.shutdownNow();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}
	
	// Shutdown and closure
	
	@Test
	public void testShutdown() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		
		service.shutdown();
		
		Future<?> future = null;
		try {
			future = service.schedule( ()-> {}, 3, TimeUnit.SECONDS);
			Assert.fail("Task should not be scheduled successfully if the Scheduled service is shut down");
		} catch (RejectedExecutionException e) {
		}
		
		Assert.assertNull("Future must be undefined", future);

		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(5, TimeUnit.SECONDS));
	}

	@Test
	public void testShutdownNowActive() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean taskCompleted = new AtomicBoolean();
		final AtomicBoolean taskWasActive = new AtomicBoolean();
		
		Future<?> future = service.schedule( ()-> {
			taskWasActive.set(true);
			taskCompleted.set(sleep(10_000));
		}, 2, TimeUnit.SECONDS);
		
		sleep(5_000);
		
		service.shutdownNow();
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(5, TimeUnit.SECONDS));

		Assert.assertTrue("Task must be active", taskWasActive.get());
		Assert.assertFalse("Task must not be completed", taskCompleted.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertFalse("Future must not be cancelled", future.isCancelled());
		Assert.assertEquals("Future must be in SUCCESS state ", Future.State.SUCCESS, future.state());
	}

	@Test
	public void testShutdownNowBeforeSubmit() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		
		service.shutdownNow();
		
		Future<?> future = null; 
		try {
			future = service.schedule( ()-> {}, 3, TimeUnit.SECONDS);
			Assert.fail("Task should not be scheduled successfully if the Scheduled service is shut down");
		} catch (RejectedExecutionException e) {
		}

		Assert.assertNull("Future must be undefined", future);

		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(5, TimeUnit.SECONDS));
	}

	@Test
	public void testClose() throws InterruptedException, ExecutionException {
		final ScheduledExecutorService service = getScheduler();
		final AtomicBoolean isVirtual = new AtomicBoolean();
		
		final Future<?> future = service.schedule( ()->  {
			isVirtual.set(Thread.currentThread().isVirtual());
			sleep(1000);
		}, 0, TimeUnit.SECONDS);
		
		future.get();

		service.close();
		Assert.assertTrue("Scheduled service must terminate", service.isTerminated());

		Assert.assertTrue("Task must be executed on virtual thread", isVirtual.get());
		Assert.assertTrue("Future must be done", future.isDone());
		Assert.assertEquals("Future must be in succeeded state", Future.State.SUCCESS, future.state());
		Assert.assertFalse("Future must not be cancelled", future.isCancelled());
		Assert.assertTrue("Scheduled service must terminate", service.awaitTermination(1, TimeUnit.SECONDS));
	}

}
