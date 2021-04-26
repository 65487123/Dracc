 /* Copyright zeping lu
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  *  Unless required by applicable law or agreed to in writing, software
  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  See the License for the specific language governing permissions and
  *  limitations under the License.
  */

package com.lzp.registry.server.util;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

 /**
  * Description:线程池，和jdk的线程池用法一样
  * 核心线程从创建后就一直存在，直到线程池被关闭，额外线程空闲一段时间就会死亡,超时单位不用填，只能是秒
  * <p>
  * 相比{@link java.util.concurrent.ThreadPoolExecutor}的优势
  * <p>
  * 1、达到最大线程数时：
  * JUC自带的线程池：当核心线程数满了，队列也满了，这时候还有大量任务进来。处理这些任务时发现当前
  * 线程数还没达到最大线程数，这些任务会争夺创建额外线程的权利，当没有抢到创建额外线程的权利，直接执行拒绝策略。
  * 这个线程池：当核心线程数满了，队列也满了，这时候还有大量任务进来，同样这些任务会争夺创建额外线程的权利，
  * 但是，如果没有抢到争夺额外线程的权利，会再次把任务丢进阻塞队列一次,如果队列还是满的，才会执行拒绝策略。(虽
  * 然第二次能加进去的概率很小很小...)
  * <p>
  * 2、性能高(执行任务的额外耗时小)
  * 简单自测了下，execute()大量小任务，性能比{@link java.util.concurrent.ThreadPoolExecutor}要高很多。
  * 由于性能比jdk自带的线程池高，进一步降低了执行拒绝策略的概率(这才是主要原因）
  * <p>
  * 经过实际测试，使用长度500万的有界队列，使用默认的拒绝策略(抛异常),核心线程数为4，最大线程数为8，
  * 执行1亿个小任务，JDK自带的线程池，几乎百分百抛出拒绝策略的异常，这个线程池几乎百分百完成了所有
  * 任务而没有执行拒绝策略。(测试结果和机器有关，总的来说，当队列容量远小于总任务数量，核心线程数量又小于
  * 最大线程数时，执行拒绝策略的概率比JDK自带的线程池小很多)
  * <p>
  * 3、重写了submit(),返回的Future可以增加异步回调方法
  * JDK自带的线程池，执行submit()返回的是{@link FutureTask}对象，这个对象获取结果需要阻塞等待，
  * 而这个返回的是{@link ListenableFuture},这个future能添加异步回调方法，当任务执行结束，会执行回调方法。
  *
  * @author: Lu ZePing
  * @date: 2019/6/2 15:19
  */
 public class ThreadPoolExecutor implements ExecutorService {

     /**
      * 核心线程数量
      */
     private final int CORE_NUM;

     /**
      * 最大线程数量
      */
     private final int MAX_NUM;

     /**
      * 阻塞队列
      */
     private BlockingQueue<Runnable> blockingQueue;

     /**
      * 额外线程最大空闲时间
      */
     private int keepAliveTime;

     /**
      * 线程工厂
      */
     private ThreadFactory threadFactory;

     /**
      * 拒绝策略
      */
     private RejectExecuHandler rejectedExecutionHandler;

     /**
      * 当前的工作线程数量
      */
     private AtomicInteger workerSum = new AtomicInteger(0);

     /**
      * 当前的工作线程
      */
     private final List<Worker> WORKER_LIST;

     /**
      * 标志核心线程是否已满
      */
     private volatile boolean coreThreadMax;

     /**
      * 标志额外线程是否已满
      */
     private volatile boolean additionThreadMax = false;

     /**
      * 标志线程池是否已经被关闭(调用shutDown())
      */
     private volatile boolean shutdown;

     /**
      * 标志线程池是否已经被立即关闭(调用shutDownNow())，或者调用shutDown()后并且队列没任务
      */
     private volatile boolean shutdownNow;

     /**
      * Description:工作线程的封装
      */
     class Worker implements Runnable {
         /**
          * 工作线程的第一个任务
          * 用volatile修饰主要是为了防止shutDown()出问题
          */
         private volatile Runnable firstTask;
         /**
          * 标志是否是额外线程
          */
         private boolean additional;
         /**
          * 工作线程的引用
          */
         private Thread thread;

         Worker(Runnable firstTask, boolean additional) {
             this.firstTask = firstTask;
             this.additional = additional;
             thread = threadFactory.newThread(this);
             thread.start();
         }

         @Override
         public void run() {
             try {
                 if (additional) {
                     do {
                         firstTask.run();
                         firstTask = null;
                     }
                     while ((firstTask = blockingQueue.poll(keepAliveTime, TimeUnit.SECONDS)) != null);
                     synchronized (WORKER_LIST) {
                         WORKER_LIST.remove(this);
                     }
                     workerSum.decrementAndGet();
                     additionThreadMax = false;
                 } else {
                     //java中,while(true)和for(;;)编译后生成的字节码一模一样
                     while (true) {
                         firstTask.run();
                         firstTask = null;
                         firstTask = blockingQueue.take();
                     }
                 }
             } catch (Throwable t) {
                 onCatchThrowable(t);
             }
         }

         private void onCatchThrowable(Throwable t) {
             synchronized (WORKER_LIST) {
                 WORKER_LIST.remove(this);
             }
             //判断是否是调用了shutdownNow()方法而导致的线程中断
             if (!shutdownNow) {
                 Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
                 //如果execute的任务抛出了未捕捉的异常，可以通过线程工厂创建线程的时候设置UncaughtExceptionHandler来捕捉
                 if ((uncaughtExceptionHandler = this.thread.getUncaughtExceptionHandler()) != null) {
                     uncaughtExceptionHandler.uncaughtException(this.thread, t);
                 }
                 if (this.additional) {
                     workerSum.decrementAndGet();
                     additionThreadMax = false;
                 } else {
                     Runnable firstTask;
                     while (!shutdownNow) {
                         try {
                             if ((firstTask = blockingQueue.poll(5, TimeUnit.SECONDS)) != null) {
                                 addWorker(firstTask, false);
                                 break;
                             }
                         } catch (InterruptedException ignored) {
                         }
                     }
                 }
             }
         }
     }

     /**
      * 抛出{@code RejectedExecutionException}的拒绝策略
      */
     static class AbortPolicy implements RejectExecuHandler {

         /**
          * 总是会抛出 RejectedExecutionException.
          *
          * @param r        需要执行的任务
          * @param executor 试图执行这个任务的执行器
          * @throws RejectedExecutionException always
          */
         @Override
         public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
             throw new RejectedExecutionException("Task " + r.toString() +
                     " rejected from " +
                     executor.toString());
         }
     }

     /**
      * 线程池构造器，额外线程超时时间单位为秒
      *
      * @param coreNum                  核心线程数
      * @param maxNum                   最大线程数
      * @param keepAliveTime            额外线程最大空闲时间
      * @param blockingQueue            阻塞队列
      * @param threadFactory            线程工厂
      * @param rejectedExecutionHandler 拒绝策略
      */
     public ThreadPoolExecutor(int coreNum, int maxNum, int keepAliveTime, BlockingQueue<Runnable> blockingQueue, ThreadFactory threadFactory, RejectExecuHandler rejectedExecutionHandler) {
         if (coreNum < 0 || maxNum <= 0 || maxNum
                 < coreNum || keepAliveTime < 0) {
             throw new IllegalArgumentException();
         }
         this.CORE_NUM = coreNum;
         this.MAX_NUM = maxNum;
         this.blockingQueue = blockingQueue;
         this.threadFactory = threadFactory;
         this.rejectedExecutionHandler = rejectedExecutionHandler;
         this.keepAliveTime = keepAliveTime;
         WORKER_LIST = new ArrayList<>(CORE_NUM);
         coreThreadMax = (coreNum == 0);
     }


     /**
      * 线程池构造器，额外线程超时时间单位为秒
      * 使用默认拒绝策略(抛异常),默认线程工厂
      */
     public ThreadPoolExecutor(int coreNum, int maxNum, int keepAliveTime, BlockingQueue<Runnable> blockingQueue) {
         this(coreNum, maxNum, keepAliveTime, blockingQueue, Executors.defaultThreadFactory(), new AbortPolicy());
     }

     /**
      * 线程池构造器，额外线程超时时间单位为秒
      * 使用默认线程工厂
      */
     public ThreadPoolExecutor(int coreNum, int maxNum, int keepAliveTime, BlockingQueue<Runnable> blockingQueue, RejectExecuHandler rejectedExecutionHandler) {
         this(coreNum, maxNum, keepAliveTime, blockingQueue, Executors.defaultThreadFactory(), rejectedExecutionHandler);
     }

     /**
      * 线程池构造器，额外线程超时时间单位为秒
      * 使用默认拒绝策略(抛异常)
      */
     public ThreadPoolExecutor(int coreNum, int maxNum, int keepAliveTime, BlockingQueue<Runnable> blockingQueue, ThreadFactory threadFactory) {
         this(coreNum, maxNum, keepAliveTime, blockingQueue, threadFactory, new AbortPolicy());
     }

     @Override
     public void execute(Runnable command) {
         if (shutdown) {
             rejectedExecutionHandler.rejectedExecution(command, this);
             //判断线程数是否已经达到核心线程数
         } else if (coreThreadMax) {
             //当核心线程数已满
             branchForCoreThreadMax(command);
         } else {
             //当核心线程数未满
             branchForCoreThrNotMax(command);
         }
     }

     /**
      * 如果线程数量已经达到核心线程数,进入这个方法
      */
     private void branchForCoreThreadMax(Runnable command) {
         if (blockingQueue.offer(command)) {
             if (CORE_NUM == 0) {
                 addWorkerIfIfNecessary(command);
             }
         } else {
             //核心线程数满了,队列也满了,判断线程数是否已经达到最大线程数
             if (additionThreadMax) {
                 rejectedExecutionHandler.rejectedExecution(command, this);
             } else {
                 //没达到最大线程数,进行cas,然后看是否抢到创建额外线程的权利
                 if (workerSum.getAndIncrement() >= MAX_NUM) {
                     //没抢到,线程数量减回去
                     workerSum.getAndDecrement();
                     //执行拒绝策略前再入队一次，不成功就执行拒绝策略
                     if (!blockingQueue.offer(command)) {
                         rejectedExecutionHandler.rejectedExecution(command, this);
                     }
                 } else {
                     //抢到了创建额外线程权力
                     if (workerSum.get() == MAX_NUM) {
                         this.additionThreadMax = true;
                     }
                     addWorker(command, true);
                 }
             }
         }
     }

     private void addWorkerIfIfNecessary(Runnable command) {
         synchronized (WORKER_LIST) {
             if (WORKER_LIST.isEmpty() && blockingQueue.contains(command)) {
                 addWorker(() -> {
                 }, true);
                 return;
             } else {
                 for (Worker worker : WORKER_LIST) {
                     if (worker.firstTask != null) {
                         return;
                     }
                 }
             }
         }
         //如果队列中这个元素一直没被取走,线程池里的线程最终肯定会全死光
         while (blockingQueue.contains(command)) {
             synchronized (WORKER_LIST) {
                 if (WORKER_LIST.isEmpty()) {
                     addWorker(() -> {
                     }, true);
                 }
             }
         }
     }


     private void addWorker(Runnable command, boolean additional) {
         synchronized (WORKER_LIST) {
             if (!shutdownNow) {
                 WORKER_LIST.add(new Worker(command, additional));
             }
         }
     }

     /**
      * 如果线程数量还没达到核心线程数量,进入这个方法
      */
     private void branchForCoreThrNotMax(Runnable command) {
         //线程数量没达到核心线程数，争抢创建核心线程机会
         if (workerSum.getAndIncrement() >= CORE_NUM) {
             //没抢到创建核心线程机会，入队
             if (blockingQueue.offer(command)) {
                 workerSum.getAndDecrement();
             } else {
                 //发现队列已满，查看是否抢到创建额外线程机会
                 if (workerSum.get() <= MAX_NUM) {
                     addWorker(command, true);
                 } else {
                     workerSum.getAndDecrement();
                     rejectedExecutionHandler.rejectedExecution(command, this);
                 }
             }
         } else {
             //抢到创建核心线程机会
             if (workerSum.intValue() == CORE_NUM) {
                 this.coreThreadMax = true;
             }
             addWorker(command, false);
         }
     }

     @Override
     public List<Runnable> shutdownNow() {
         this.shutdown = true;
         this.shutdownNow = true;
         while (!WORKER_LIST.isEmpty()) {
             synchronized (WORKER_LIST) {
                 for (Worker worker : WORKER_LIST) {
                     worker.thread.interrupt();
                 }
             }
             try {
                 Thread.sleep(1);
             } catch (InterruptedException ignored) {
             }
         }
         synchronized (this) {
             this.notifyAll();
         }
         return new ArrayList(blockingQueue);
     }


     /**
      * Returns the current number of threads in the pool.
      *
      * @return the number of threads
      */
     public int getPoolSize() {
         synchronized (WORKER_LIST) {
             return WORKER_LIST.size();
         }
     }


     @Override
     public void shutdown() {
         if (!shutdown) {
             this.shutdown = true;
             ThreadPoolExecutor executorService = this;
             if (CORE_NUM == 0) {
                 newThreadToTerminate(this);
             } else if (!blockingQueue.offer(executorService::stop)) {
                 if (blockingQueue.isEmpty()) {
                     //大概率是队列重写了offer方法
                     newThreadToTerminate(this);
                 } else {
                     while (!blockingQueue.offer(executorService::stop)) {
                         try {
                             Thread.sleep(1);
                         } catch (InterruptedException ignored) {
                         }
                     }
                 }
             }
         }
     }


     private void newThreadToTerminate(ThreadPoolExecutor thisExecutor) {
         new Thread(() -> {
             while (!blockingQueue.isEmpty()) {
                 try {
                     Thread.sleep(10);
                 } catch (InterruptedException ignored) {
                 }
             }
             thisExecutor.shutdownNow = true;
             while (!WORKER_LIST.isEmpty()) {
                 synchronized (WORKER_LIST) {
                     for (Worker worker : WORKER_LIST) {
                         if (worker.firstTask == null) {
                             worker.thread.interrupt();
                         }
                     }
                 }
                 try {
                     Thread.sleep(10);
                 } catch (InterruptedException ignored) {
                 }
             }
             synchronized (thisExecutor) {
                 thisExecutor.notifyAll();
             }
         }).start();
     }


     @Override
     public boolean isShutdown() {
         return shutdown;
     }

     @Override
     public boolean isTerminated() {
         return shutdownNow && this.WORKER_LIST.isEmpty();
     }

     @Override
     public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
         if (shutdownNow && this.WORKER_LIST.isEmpty()) {
             return true;
         } else {
             synchronized (this) {
                 if (shutdownNow && this.WORKER_LIST.isEmpty()) {
                     return true;
                 } else {
                     long remainingTime = unit.toMillis(timeout);
                     long deadLine = System.currentTimeMillis() + remainingTime;
                     while (!(shutdownNow && this.WORKER_LIST.isEmpty()) && remainingTime > 0) {
                         this.wait(remainingTime);
                         remainingTime = deadLine - System.currentTimeMillis();
                     }
                     return remainingTime <= 0;
                 }
             }
         }
     }

     @Override
     public <T> ListenableFuture<T> submit(Callable<T> task) {
         ListenableFuture<T> listenableFuture = new ListenableFuture(task);
         this.execute(listenableFuture);
         return listenableFuture;
     }


     @Override
     public <T> Future<T> submit(Runnable task, T result) {
         ListenableFuture<T> listenableFuture = new ListenableFuture(() -> {
             task.run();
             return result;
         });
         this.execute(listenableFuture);
         return listenableFuture;
     }

     @Override
     public Future<?> submit(Runnable task) {
         ListenableFuture<?> listenableFuture = new ListenableFuture(() -> {
             task.run();
             return null;
         });
         this.execute(listenableFuture);
         return listenableFuture;
     }


     @Override
     public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
         if (tasks == null) {
             throw new NullPointerException();
         }
         ArrayList<Future<T>> futures = new ArrayList<>(tasks.size());
         boolean done = false;
         try {
             for (Callable<T> t : tasks) {
                 RunnableFuture<T> f = new FutureTask<>(t);
                 futures.add(f);
                 execute(f);
             }
             for (Future<T> f : futures) {
                 if (!f.isDone()) {
                     try {
                         f.get();
                     } catch (CancellationException | ExecutionException ignore) {
                     }
                 }
             }
             done = true;
             return futures;
         } finally {
             if (!done) {
                 for (Future<T> future : futures) {
                     future.cancel(true);
                 }
             }
         }
     }

     @Override
     public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
         if (tasks == null) {
             throw new NullPointerException();
         }
         long nanos = unit.toNanos(timeout);
         ArrayList<Future<T>> futures = new ArrayList<>(tasks.size());
         boolean done = false;
         try {
             for (Callable<T> t : tasks) {
                 futures.add(new FutureTask<>(t));
             }

             final long deadline = System.nanoTime() + nanos;
             final int size = futures.size();

             // Interleave time checks and calls to execute in case
             // executor doesn't have any/much parallelism.
             for (int i = 0; i < size; i++) {
                 execute((Runnable) futures.get(i));
                 nanos = deadline - System.nanoTime();
                 if (nanos <= 0L) {
                     return futures;
                 }
             }

             for (int i = 0; i < size; i++) {
                 Future<T> f = futures.get(i);
                 if (!f.isDone()) {
                     if (nanos <= 0L) {
                         return futures;
                     }
                     try {
                         f.get(nanos, TimeUnit.NANOSECONDS);
                     } catch (CancellationException | ExecutionException ignore) {
                     } catch (TimeoutException toe) {
                         return futures;
                     }
                     nanos = deadline - System.nanoTime();
                 }
             }
             done = true;
             return futures;
         } finally {
             if (!done) {
                 for (Future<T> future : futures) {
                     future.cancel(true);
                 }
             }
         }
     }

     /**
      * 用处不大，并且就算有这种需求，也可以自己根据业务需求去选择更好的实现
      */
     @Override
     public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
         throw new UnsupportedOperationException();
     }

     /**
      * 用处不大，并且就算有这种需求，也可以自己根据业务需求去选择更好的实现
      */
     @Override
     public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
         throw new UnsupportedOperationException();
     }

     /**
      * 调用shutDown时会用到，中断空闲的线程
      */
     protected void stop() {
         this.shutdownNow = true;
         while (WORKER_LIST.size() != 1) {
             synchronized (WORKER_LIST) {
                 for (Worker worker : WORKER_LIST) {
                     if (worker.firstTask == null) {
                         worker.thread.interrupt();
                     }
                 }
             }
             try {
                 Thread.sleep(1);
             } catch (InterruptedException ignored) {
             }
         }
         WORKER_LIST.clear();
         Thread.currentThread().interrupt();
         synchronized (this) {
             this.notifyAll();
         }
     }


     protected BlockingQueue getBlockingQueue() {
         return this.blockingQueue;
     }

     protected void setShutdown() {
         this.shutdown = true;
     }
 }
