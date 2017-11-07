/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.concurrent;


import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoop;
import io.netty.channel.epoll.Native;
import io.netty.util.concurrent.AbstractScheduledEventExecutor;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.reactivex.plugins.RxJavaPlugins;
import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.jctools.queues.MpscArrayQueue;
import sun.misc.Contended;

/**
 * A TPC event loop group that uses EPoll for I/O tasks.
 */
public class EpollTPCEventLoopGroup extends MultithreadEventLoopGroup implements TPCEventLoopGroup
{
    private static final String DEBUG_RUNNING_TIME_NAME = "dse.tpc.debug_task_running_time_seconds";
    private static final long DEBUG_RUNNING_TIME_NANOS = TimeUnit.SECONDS.toNanos(Integer.parseInt(System.getProperty(DEBUG_RUNNING_TIME_NAME, "0")));
    private static final Logger LOGGER = LoggerFactory.getLogger(EpollTPCEventLoopGroup.class);

    // all the values set here are not as well researched as they should be... but the reasoning is in the javadoc

    /**
     * Scheduling granularity below 1us is not productive. Setting this value high however will delay scheduled channel
     * events such as flush. We use this to throttle calls to {@link AbstractScheduledEventExecutor#hasScheduledTasks()}
     * and {@link AbstractScheduledEventExecutor#pollScheduledTask(long)}.
     */
    private static final long SCHEDULED_CHECK_INTERVAL_NANOS = Long.parseLong(System.getProperty("netty.schedule_check_interval_nanos", "1000"));

    /**
     * Calling Epoll.selectNow is a systemcall, we are reluctant to saturate. Setting this value low will result in more
     * system calls. Setting it high will delay the discovery of channel events.
     */
    private static final long EPOLL_CHECK_INTERVAL_NANOS = Long.parseLong(System.getProperty("netty.epoll_check_interval_nanos", "2000"));
    private static final boolean DO_EPOLL_CHECK = EPOLL_CHECK_INTERVAL_NANOS != -1;

    /**
     * We have a backoff scheme going from busy spinning to yield to parkNanos(1) to epollWait
     * (see {@link SingleCoreEventLoop#backoff(int)}). Experimenting shows that yield and short sleeps result in high
     * number of context switches and end up hurting latency and throughput. The defaults reflect this finding, but
     * may not be suitable for every occasion. Tune with great care...
     * Can use special values below to either set a stage as the last or skip it.
     */
    private static final long BUSY_BACKOFF = Long.parseLong(System.getProperty("netty.eventloop.busy_extra_spins", "10")); // x5ns
    private static final long YIELD_BACKOFF = Long.parseLong(System.getProperty("netty.eventloop.yield_extra_spins", "0")); // x5us
    private static final long PARK_BACKOFF = Long.parseLong(System.getProperty("netty.eventloop.park_extra_spins", "0")); // x50us

    private static final long SKIP_BACKOFF_STAGE = 0;
    private static final long LAST_BACKOFF_STAGE = -1;

    @Contended
    private final ImmutableList<SingleCoreEventLoop> eventLoops;

    private volatile boolean shutdown;

    /**
     * Constructor used by jmh through reflection (so it must have this exact signature, even though the name is
     * ignored) in some benchmarks (grep for "-Djmh.executor.class=org.apache.cassandra.concurrent.EpollTPCEventLoopGroup"
     * to see where it's used).
     */
    @VisibleForTesting
    public EpollTPCEventLoopGroup(int nThreads, String name)
    {
        this(nThreads);
    }

    /**
     * Creates new a {@code EpollTPCEventLoopGroup} using the provided number of event loops.
     *
     * @param nThreads the number of event loops to use (not that every loop is exactly one thread, but the group also
     *                 use an additional thread for monitoring).
     */
    public EpollTPCEventLoopGroup(int nThreads)
    {
        super(nThreads, TPCThread.newTPCThreadFactory());

        this.eventLoops = ImmutableList.copyOf(Iterables.transform(this, e -> (SingleCoreEventLoop) e));

        //Register these loop threads with the Watcher
        WatcherThread.instance.get().addThreadsToMonitor(new ArrayList<>(eventLoops));
    }

    public ImmutableList<? extends TPCEventLoop> eventLoops()
    {
        return eventLoops;
    }

    @Override
    public void shutdown()
    {
        super.shutdown();

        WatcherThread.instance.get().removeThreadsToMonitor(new ArrayList<>(eventLoops));
        shutdown = true;
    }

    protected EventLoop newChild(Executor executor, Object... args) throws Exception
    {
        assert executor instanceof TPCThread.TPCThreadsCreator;
        return new SingleCoreEventLoop(this, (TPCThread.TPCThreadsCreator)executor);
    }

    public static class SingleCoreEventLoop extends EpollEventLoop implements TPCEventLoop, WatcherThread.MonitorableThread
    {
        /**
         * debug purposes only.
         */
        private volatile long lastDrainTime;

        private final EpollTPCEventLoopGroup parent;
        private final TPCThread thread;
        private final TPCMetrics metrics;
        private final TPCMetricsAndLimits.TaskStats busySpinStats;
        private final TPCMetricsAndLimits.TaskStats yieldStats;
        private final TPCMetricsAndLimits.TaskStats parkStats;
        private final MpscArrayQueue<Runnable> queue;
        private final MpscArrayQueue<TPCRunnable> pendingQueue;

        @Contended
        private volatile ThreadState state;

        // used to ensure correct initializing on racy thread start
        private final CountDownLatch racyInit = new CountDownLatch(1);

        /**
         * tracks the return value of the select methods and when not zero triggers a
         * {@link SingleCoreEventLoop#processEpollEvents()}
         */
        private int pendingEpollEvents = 0;

        /** for epoll throttling */
        private long lastEpollCheckTime = nanoTimeSinceStartup();

        /** for schedule check throttling */
        private long lastScheduledCheckTime = lastEpollCheckTime;

        private SingleCoreEventLoop(EpollTPCEventLoopGroup parent, TPCThread.TPCThreadsCreator executor)
        {
            super(parent,
                  executor,
                  0,
                  DefaultSelectStrategyFactory.INSTANCE.newSelectStrategy(),
                  RejectedExecutionHandlers.reject(),
                  TPC.USE_AIO);

            this.parent = parent;
            this.queue = new MpscArrayQueue<>(1 << 16);
            this.pendingQueue = new MpscArrayQueue<>(DatabaseDescriptor.getTPCPendingRequestsLimit());

            this.state = ThreadState.WORKING;
            this.lastDrainTime = -1;

            // Start the loop, which forces the creation of the Thread using 'executor' so we can get a reference to it
            // easily.
            submit(() -> {});

            this.thread = executor.lastCreatedThread();
            assert this.thread != null;
            TPCMetricsAndLimits metrics = (TPCMetricsAndLimits) this.thread.metrics();

            this.metrics = metrics;
            this.busySpinStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_SPIN);
            this.yieldStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_YIELD);
            this.parkStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_PARK);
            // prevent a racing thread start from seeing half initialized instance
            racyInit.countDown();
        }

        @Override
        public TPCThread thread()
        {
            return thread;
        }

        @Override
        public TPCEventLoopGroup parent()
        {
            return parent;
        }

        /**
         * We want to make sure netty doesn't wake up epoll when we
         * add a task.
         */
        @Override
        public boolean wakesUpForTask(Runnable task)
        {
            return false;
        }

        /**
         * Called from {@link WatcherThread} if {@link #shouldUnpark(long)} returns true. Note that an epoll event
         * will wake this thread up independently from this.
         */
        @Override
        public void unpark()
        {
            // Racy wakeups are not a concern since we have a single watcher thread, and single epoll thread both
            // willing to retry. If a Watcher wakes up the selector twice, processReady will drop the events in any
            // case. The selector waking up independantly will result in the next select returning immediately, which
            // while wasteful is harmless and rare.
            Native.eventFdWrite(this.eventFd.intValue(), 1L);
        }

        /**
         * Called regularly from {@link WatcherThread}.
         */
        @Override
        public boolean shouldUnpark(long nanoTimeSinceStartup)
        {
            if (DEBUG_RUNNING_TIME_NANOS > 0 && state == ThreadState.WORKING)
            {
                checkLongRunningTasks(nanoTimeSinceStartup);
            }

            // The park method covers waking up for epoll events and the nearest scheduling event. The likelihood of
            // pending events with no queue tasks is also very low (almost impossible), but not entirely impossible.
            // Note that since the scheduled tasks queue is not thread safe it would in any case be wrong to check it
            // from this thread.
            return state == ThreadState.PARKED && (hasQueueTasks() || hasPendingTasks());
        }

        @Override
        public boolean canExecuteImmediately(TPCRunnable task)
        {
            if (coreId() != TPC.getCoreId())
                return false;
            if (!task.isPendable())
                return true;
            if (!pendingQueue.isEmpty())
                return false;
            if (queue.size() >= metrics.maxQueueSize())
                return false;
            return true;
        }

        /**
         * The actual event loop. Each loop consists in checking for both events on epoll and tasks on our internal/external/schedule queues.
         * Any available work is executed and we then loop. When work is not available we backoff gradually until we
         * {@link SingleCoreEventLoop#parkOnEpollWait()}
         */
        @Override
        protected void run()
        {
            // prevent a racing thread start from seeing half initialized instance
            Uninterruptibles.awaitUninterruptibly(racyInit);
            while (!parent.shutdown)
            {
                try
                {
                    if (processEvents(nanoTimeSinceStartup()) == 0)
                    {
                        waitForWork();
                    }
                }
                catch (Throwable e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    LOGGER.error("Error in event loop:", e);
                }
            }

            if (isShuttingDown())
            {
                closeAll();
                confirmShutdown();
            }
        }

        /**
         * JCTools queues don't support remove. The return value reflects the expected behaviour from
         * {@link SingleCoreEventLoop#execute} where remove is called immediately after an add.
         */
        @Override
        protected boolean removeTask(Runnable task)
        {
            return true;
        }

        @Override
        protected void addTask(Runnable task)
        {
            if (task instanceof TPCRunnable)
            {
                TPCRunnable tpc = (TPCRunnable) task;
                if (tpc.isPendable())
                {
                    // If we already have something in the pending queue, this task should not jump it.
                    if (pendingQueue.isEmpty() && queue.offerIfBelowThreshold(task, metrics.maxQueueSize()))
                        return;

                    if (pendingQueue.relaxedOffer(tpc))
                    {
                        tpc.setPending();
                        return;
                    }
                    else
                    {
                        tpc.blocked();
                        reject(task);
                    }
                }
            }

            if (!queue.relaxedOffer(task))
                reject(task);
        }

        /**
         * This method called from {@link EpollEventLoop#epollWait(boolean)}
         */
        @Override
        protected boolean hasTasks()
        {
            assert inEventLoop();
            return hasQueueTasks() ||
                   hasPendingTasks() ||
                   throttledHasScheduledEvents(nanoTimeSinceStartup());
        }

        // Why not inline? because the loop here is potentially long running and has little to gain from the context of
        // the run loop.
        @DontInline
        private void waitForWork()
        {
            int idle = 0;
            boolean shouldContinue;
            do
            {
                shouldContinue = backoff(++idle);
            } while (!parent.shutdown && shouldContinue && isIdle());
        }

        private boolean isIdle()
        {
            if (hasQueueTasks() || hasPendingTasks())
            {
                return false;
            }
            final long nanoTimeSinceStartup = nanoTimeSinceStartup();
            return !(throttledHasScheduledEvents(nanoTimeSinceStartup) || throttledHasEpollEvents(nanoTimeSinceStartup));
        }

        private boolean throttledHasScheduledEvents(long nanoTimeSinceStartup)
        {
            if (nanoTimeSinceStartup - lastScheduledCheckTime > SCHEDULED_CHECK_INTERVAL_NANOS)
            {
                boolean result = hasScheduledTasks(nanoTimeSinceStartup);
                if (!result)
                {
                    lastScheduledCheckTime = nanoTimeSinceStartup;
                }
                return result;
            }
            return false;
        }

        private boolean throttledHasEpollEvents(long nanoTimeSinceStartup)
        {
            if (DO_EPOLL_CHECK && nanoTimeSinceStartup - lastEpollCheckTime > EPOLL_CHECK_INTERVAL_NANOS && pendingEpollEvents == 0)
            {
                epollSelectNow(nanoTimeSinceStartup);
            }
            return pendingEpollEvents != 0;
        }

        private boolean backoff(int idle)
        {
            if (BUSY_BACKOFF != SKIP_BACKOFF_STAGE &&
                (BUSY_BACKOFF == LAST_BACKOFF_STAGE || idle < BUSY_BACKOFF))
            {
                busySpinStats.scheduledTasks.add(1);
                busySpinStats.completedTasks.add(1);
            }
            else if (YIELD_BACKOFF != SKIP_BACKOFF_STAGE &&
                     (YIELD_BACKOFF == LAST_BACKOFF_STAGE || idle < BUSY_BACKOFF + YIELD_BACKOFF))
            {
                yieldStats.scheduledTasks.add(1);
                yieldStats.completedTasks.add(1);

                Thread.yield();
                // force a select
                lastEpollCheckTime = -1;
            }
            else if (PARK_BACKOFF != SKIP_BACKOFF_STAGE &&
                     (PARK_BACKOFF == LAST_BACKOFF_STAGE || idle < BUSY_BACKOFF + YIELD_BACKOFF + PARK_BACKOFF))
            {
                parkStats.scheduledTasks.add(1);
                parkStats.completedTasks.add(1);

                LockSupport.parkNanos(1);
                // force a select
                lastEpollCheckTime = -1;
            }
            else
            {
                parkOnEpollWait();
                return false;
            }
            return true;
        }

        private void parkOnEpollWait()
        {
            // epoll wait will wake up for next scheduled task/epoll event
            state = ThreadState.PARKED;
            epollSelect();
            state = ThreadState.WORKING;
        }

        private void checkLongRunningTasks(long nanoTimeSinceStartup)
        {
            if (lastDrainTime > 0 && Math.abs(nanoTimeSinceStartup - lastDrainTime) > DEBUG_RUNNING_TIME_NANOS)
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Detected task running for {} seconds for thread with stack:\n{}",
                                 TimeUnit.SECONDS.convert(Math.abs(nanoTimeSinceStartup - lastDrainTime), TimeUnit.NANOSECONDS),
                                 FBUtilities.Debug.getStackTrace(thread));
                }
                lastDrainTime = -1;
            }
        }

        // Why not inline? because the context of the run loop has nothing to add to the heavy lifting triggered from
        // this call. This is where all the work gets done.
        @DontInline
        private int processEvents(long nanoTimeSinceStartup)
        {
            if (DEBUG_RUNNING_TIME_NANOS > 0)
            {
                lastDrainTime = nanoTimeSinceStartup;
            }

            int processed = 0;
            // throttle epoll calls
            if (throttledHasEpollEvents(nanoTimeSinceStartup))
            {
                processed += processEpollEvents();
            }

            // throttle scheduled tasks check
            if (nanoTimeSinceStartup - lastScheduledCheckTime > SCHEDULED_CHECK_INTERVAL_NANOS)
            {
                processed += transferFromScheduledTasks(nanoTimeSinceStartup);
            }

            processed += processTasks();

            processed += transferFromPendingTasks();
            return  processed;
        }

        private int processEpollEvents()
        {
            final int currPendingEpollEvents = this.pendingEpollEvents;
            if (currPendingEpollEvents == 0)
            {
                return 0;
            }
            this.pendingEpollEvents = 0;
            try
            {
                processReady(events, currPendingEpollEvents);

                if (allowGrowing && currPendingEpollEvents == events.length())
                {
                    events.increase();
                }

                return currPendingEpollEvents;
            }
            catch (Exception e)
            {
                handleEpollEventError(e);
                return 0;
            }
        }

        private void epollSelect()
        {
            if (pendingEpollEvents != 0)
                throw new IllegalStateException("Should not be doing a blocking select with pendingEpollEvents="+pendingEpollEvents);


            try
            {
                // see note in {@link SingleCoreEventLoop#unpark}
                pendingEpollEvents = epollWait(false);
                lastEpollCheckTime = nanoTimeSinceStartup();
                assert pendingEpollEvents >= 0;
            }
            catch (Exception e)
            {
                LOGGER.error("Error selecting socket ", e);
            }
        }

        private void epollSelectNow(long nanoTimeSinceStartup)
        {
            if (pendingEpollEvents != 0)
                throw new IllegalStateException("Should not be doing a selectNow with pendingEpollEvents=" + pendingEpollEvents);

            lastEpollCheckTime = nanoTimeSinceStartup;

            try
            {
                pendingEpollEvents = selectNowSupplier.get();
                assert pendingEpollEvents >= 0;
            }
            catch (Exception e)
            {
                LOGGER.error("Error selecting socket ", e);
            }
        }

        private void handleEpollEventError(Exception e)
        {
            LOGGER.error("Unexpected exception in the selector loop.", e);

            // Prevent possible consecutive immediate failures that lead to
            // excessive CPU consumption.
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }

        private int processTasks()
        {
            int processed = 0;
            try
            {
                final MpscArrayQueue<Runnable> queue = this.queue;
                Runnable r;
                while (processed < Short.MAX_VALUE && (r = queue.relaxedPoll()) != null)
                {
                    r.run();
                    ++processed;
                }
            }
            catch (Throwable t)
            {
                handleTaskException(t);
            }

            return processed;
        }

        private void handleTaskException(Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);

            LOGGER.error("Task exception encountered: ", t);
            try
            {
                RxJavaPlugins.getErrorHandler().accept(t);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        private int transferFromPendingTasks()
        {
            int processed = 0;
            final int maxQueueSize = metrics.maxQueueSize();
            final MpscArrayQueue<TPCRunnable> pendingQueue = this.pendingQueue;
            TPCRunnable tpc;
            while (queue.size() < maxQueueSize && (tpc = pendingQueue.relaxedPeek()) != null)
            {
                // despite the size check this can fail as size will grow due to concurrent offers. In theory
                // relaxedOffer allows for spurius offer fails which are not related to being full, but that's
                // not the case for MpscArrayQueue.
                if (queue.relaxedOffer(tpc))
                {
                    pendingQueue.relaxedPoll();
                    tpc.unsetPending();
                }
                else
                {
                    break;
                }
            }
            return processed;
        }

        private int transferFromScheduledTasks(long nanoTimeSinceStartup)
        {
            lastScheduledCheckTime = nanoTimeSinceStartup;
            int processed = 0;
            Runnable scheduledTask = pollScheduledTask(nanoTimeSinceStartup);
            while (scheduledTask != null)
            {
                addTask(scheduledTask);
                scheduledTask = pollScheduledTask(nanoTimeSinceStartup);
                ++processed;
            }
            return processed;
        }

        private boolean hasPendingTasks()
        {
            return pendingQueue.relaxedPeek() != null;
        }

        private boolean hasQueueTasks()
        {
            return queue.relaxedPeek() != null;
        }

        private static long nanoTimeSinceStartup()
        {
            return TPC.nanoTimeSinceStartup();
        }
    }
}