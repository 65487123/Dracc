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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


 /**
  * Description:对{@link java.util.concurrent.CountDownLatch} 做了优化，
  *
  * 1、性能比他高很多
  * 2、可重用(调用reset(),计数器会复位)
  *
  *
  * @author: Lu ZePing
  * @date: 2020/11/17 23:00
  */
 public class CountDownLatch {

     private AtomicInteger atomicInteger;

     private volatile boolean reachZero = false;

     /**
      * Constructs a {@code CountDownLatch} initialized with the given count.
      *
      * @param count the number of times {@link #countDown} must be invoked
      *              before threads can pass through {@link #await}
      * @throws IllegalArgumentException if {@code count} is negative
      */
     public CountDownLatch(int count) {
         if (count < 0) {
             throw new IllegalArgumentException("count < 0");
         }
         this.atomicInteger = new AtomicInteger(count);
     }

     /**
      * Causes the current thread to wait until the latch has counted down to
      * zero, unless the thread is {@linkplain Thread#interrupt interrupted}.
      *
      * <p>If the current count is zero then this method returns immediately.
      *
      * <p>If the current count is greater than zero then the current
      * thread becomes disabled for thread scheduling purposes and lies
      * dormant until one of two things happen:
      * <ul>
      * <li>The count reaches zero due to invocations of the
      * {@link #countDown} method; or
      * <li>Some other thread {@linkplain Thread#interrupt interrupts}
      * the current thread.
      * </ul>
      *
      * <p>If the current thread:
      * <ul>
      * <li>has its interrupted status set on entry to this method; or
      * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
      * </ul>
      * then {@link InterruptedException} is thrown and the current thread's
      * interrupted status is cleared.
      *
      * @throws InterruptedException if the current thread is interrupted
      *                              while waiting
      */
     public void await() throws InterruptedException {
         synchronized (this) {
             while (!reachZero) {
                 this.wait();
             }
         }
     }

     /**
      * Causes the current thread to wait until the latch has counted down to
      * zero, unless the thread is {@linkplain Thread#interrupt interrupted},
      * or the specified waiting time elapses.
      *
      * <p>If the current count is zero then this method returns immediately
      * with the value {@code true}.
      *
      * <p>If the current count is greater than zero then the current
      * thread becomes disabled for thread scheduling purposes and lies
      * dormant until one of three things happen:
      * <ul>
      * <li>The count reaches zero due to invocations of the
      * {@link #countDown} method; or
      * <li>Some other thread {@linkplain Thread#interrupt interrupts}
      * the current thread; or
      * <li>The specified waiting time elapses.
      * </ul>
      *
      * <p>If the count reaches zero then the method returns with the
      * value {@code true}.
      *
      * <p>If the current thread:
      * <ul>
      * <li>has its interrupted status set on entry to this method; or
      * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
      * </ul>
      * then {@link InterruptedException} is thrown and the current thread's
      * interrupted status is cleared.
      *
      * <p>If the specified waiting time elapses then the value {@code false}
      * is returned.  If the time is less than or equal to zero, the method
      * will not wait at all.
      *
      * @param timeout the maximum time to wait
      * @param unit    the time unit of the {@code timeout} argument
      * @return {@code true} if the count reached zero and {@code false}
      * if the waiting time elapsed before the count reached zero
      * @throws InterruptedException if the current thread is interrupted
      *                              while waiting
      */
     public boolean await(long timeout, TimeUnit unit)
             throws InterruptedException {
         synchronized (this) {
             long remainingTime = unit.toMillis(timeout);
             long deadline = System.currentTimeMillis() + remainingTime;
             while (!reachZero && remainingTime > 0) {
                 this.wait(remainingTime);
                 remainingTime = deadline - System.currentTimeMillis();
             }
         }
         return reachZero;
     }

     /**
      * Decrements the count of the latch, releasing all waiting threads if
      * the count reaches zero.
      */
     public void countDown() {
         if (atomicInteger.decrementAndGet() == 0) {
             synchronized (this) {
                 reachZero = true;
                 this.notifyAll();
             }
         }
     }

     /**
      * Returns the current count.
      *
      * <p>This method is typically used for debugging and testing purposes.
      *
      * @return the current count
      */
     public long getCount() {
         return atomicInteger.get();
     }

     /**
      * Returns a string identifying this latch, as well as its state.
      * The state, in brackets, includes the String {@code "Count ="}
      * followed by the current count.
      *
      * @return a string identifying this latch, as well as its state
      */
     @Override
     public String toString() {
         return super.toString() + "[Count = " + atomicInteger.get() + "]";
     }

     /**
      * reset this countdownlatch
      */
     public void reset(int value) {
         reachZero = false;
         atomicInteger.set(value);
     }
 }
