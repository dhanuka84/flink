/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs;

import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class SafetyNetCloseableRegistryTest {

	private ProducerThread[] streamOpenThreads;
	private SafetyNetCloseableRegistry closeableRegistry;
	private AtomicInteger unclosedCounter;

	public void setup() {
		this.closeableRegistry = new SafetyNetCloseableRegistry();
		this.unclosedCounter = new AtomicInteger(0);
		this.streamOpenThreads = new ProducerThread[10];
		for (int i = 0; i < streamOpenThreads.length; ++i) {
			streamOpenThreads[i] = new ProducerThread(closeableRegistry, unclosedCounter, Integer.MAX_VALUE);
		}
	}

	private void startThreads(int maxStreams) {
		for (ProducerThread t : streamOpenThreads) {
			t.setMaxStreams(maxStreams);
			t.start();
		}
	}

	private void joinThreads() throws InterruptedException {
		for (Thread t : streamOpenThreads) {
			t.join();
		}
	}

	@Test
	public void testCorrectScopesForSafetyNet() throws Exception {
		Thread t1 = new Thread() {
			@Override
			public void run() {
				try {
					FileSystem fs1 = FileSystem.getLocalFileSystem();
					// ensure no safety net in place
					Assert.assertFalse(fs1 instanceof SafetyNetWrapperFileSystem);
					FileSystem.createAndSetFileSystemCloseableRegistryForThread();
					fs1 = FileSystem.getLocalFileSystem();
					// ensure safety net is in place now
					Assert.assertTrue(fs1 instanceof SafetyNetWrapperFileSystem);
					Path tmp = new Path(fs1.getWorkingDirectory(), UUID.randomUUID().toString());
					try (FSDataOutputStream stream = fs1.create(tmp, false)) {
						Thread t2 = new Thread() {
							@Override
							public void run() {
								FileSystem fs2 = FileSystem.getLocalFileSystem();
								// ensure the safety net does not leak here
								Assert.assertFalse(fs2 instanceof SafetyNetWrapperFileSystem);
								FileSystem.createAndSetFileSystemCloseableRegistryForThread();
								fs2 = FileSystem.getLocalFileSystem();
								// ensure we can bring another safety net in place
								Assert.assertTrue(fs2 instanceof SafetyNetWrapperFileSystem);
								FileSystem.closeAndDisposeFileSystemCloseableRegistryForThread();
								fs2 = FileSystem.getLocalFileSystem();
								// and that we can remove it again
								Assert.assertFalse(fs2 instanceof SafetyNetWrapperFileSystem);
							}
						};
						t2.start();
						try {
							t2.join();
						} catch (InterruptedException e) {
							Assert.fail();
						}

						//ensure stream is still open and was never closed by any interferences
						stream.write(42);
						FileSystem.closeAndDisposeFileSystemCloseableRegistryForThread();

						// ensure leaking stream was closed
						try {
							stream.write(43);
							Assert.fail();
						} catch (IOException ignore) {

						}
						fs1 = FileSystem.getLocalFileSystem();
						// ensure safety net was removed
						Assert.assertFalse(fs1 instanceof SafetyNetWrapperFileSystem);
					} finally {
						fs1.delete(tmp, false);
					}
				} catch (Exception e) {
					e.printStackTrace();
					Assert.fail();
				}
			}
		};
		t1.start();
		t1.join();
	}

	@Test
	public void testClose() throws Exception {

		setup();
		startThreads(Integer.MAX_VALUE);

		for (int i = 0; i < 5; ++i) {
			System.gc();
			Thread.sleep(40);
		}

		closeableRegistry.close();

		joinThreads();

		Assert.assertEquals(0, unclosedCounter.get());

		try {

			WrappingProxyCloseable<Closeable> testCloseable = new WrappingProxyCloseable<Closeable>() {
				@Override
				public Closeable getWrappedDelegate() {
					return this;
				}

				@Override
				public void close() throws IOException {
					unclosedCounter.incrementAndGet();
				}
			};

			closeableRegistry.registerClosable(testCloseable);

			Assert.fail("Closed registry should not accept closeables!");

		} catch (IOException expected) {
			//expected
		}

		Assert.assertEquals(1, unclosedCounter.get());
	}

	@Test
	public void testSafetyNetClose() throws Exception {
		setup();
		startThreads(20);

		joinThreads();

		for (int i = 0; i < 5 && unclosedCounter.get() > 0; ++i) {
			System.gc();
			Thread.sleep(50);
		}

		Assert.assertEquals(0, unclosedCounter.get());
		closeableRegistry.close();
	}

	private static final class ProducerThread extends Thread {

		private final SafetyNetCloseableRegistry registry;
		private final AtomicInteger refCount;
		private int maxStreams;

		public ProducerThread(SafetyNetCloseableRegistry registry, AtomicInteger refCount, int maxStreams) {
			this.registry = registry;
			this.refCount = refCount;
			this.maxStreams = maxStreams;
		}

		public int getMaxStreams() {
			return maxStreams;
		}

		public void setMaxStreams(int maxStreams) {
			this.maxStreams = maxStreams;
		}

		@Override
		public void run() {
			try {
				int count = 0;
				while (maxStreams > 0) {
					String debug = Thread.currentThread().getName() + " " + count;
					TestStream testStream = new TestStream(refCount);
					refCount.incrementAndGet();
					ClosingFSDataInputStream pis = ClosingFSDataInputStream.wrapSafe(testStream, registry, debug); //reference dies here

					try {
						Thread.sleep(2);
					} catch (InterruptedException e) {

					}

					if (maxStreams != Integer.MAX_VALUE) {
						--maxStreams;
					}
					++count;
				}
			} catch (Exception ex) {

			}
		}
	}

	private static final class TestStream extends FSDataInputStream {

		private AtomicInteger refCount;

		public TestStream(AtomicInteger refCount) {
			this.refCount = refCount;
		}

		@Override
		public void seek(long desired) throws IOException {

		}

		@Override
		public long getPos() throws IOException {
			return 0;
		}

		@Override
		public int read() throws IOException {
			return 0;
		}

		@Override
		public void close() throws IOException {
			if (refCount != null) {
				refCount.decrementAndGet();
				refCount = null;
			}
		}
	}
}