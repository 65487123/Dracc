
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


package com.lzp.dracc.server.util;

import sun.misc.Unsafe;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Description:线程安全的ArrayList
 * jdk提供线程安全List的局限性：
 * 1、{@link Vector}:所有操作全加锁，读和写不能同时进行
 * 2、Collections.synchronizedList 和Vector一样，所有操作加锁，多线程读写时性能很低
 * 3、{@link java.util.concurrent.CopyOnWriteArrayList}：
 * 写时复制，大量写操作时，频繁new数组并复制，严重影响性能，数组元素多时很容易造成OOM
 * 适合读多写少(最好是几乎不用写,全都是读操作)
 *
 * 这个list的特点：
 * 写时加锁，读时无锁(通过Unsafe直接读内存值),适合写多读少或者读写频率差不多
 *
 * @author: Zeping Lu
 * @date: 2020/11/25 14:45
 */

public class ConcurrentArrayList<E> implements List<E>, Serializable, Iterable<E> {

    private static final long serialVersionUID = 8681452561125892189L;

    private final long BASE;

    private final int ASHIFT;

    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private static final Unsafe U;

    private transient Object[] elementData;

    private volatile int size;

    private class Itr implements Iterator<E> {

        private Object[] elementDataView;
        private int index = 0;

        {
            synchronized (this) {
                elementDataView = new Object[size];
                System.arraycopy(elementData, 0, elementDataView, 0, size);
            }
        }

        @Override
        public boolean hasNext() {
            return index != elementDataView.length;
        }

        @Override
        public E next() {
            return (E) elementDataView[index++];
        }
    }

    static {
        try {
            Constructor<Unsafe> constructor = Unsafe.class.getDeclaredConstructor();
            constructor.setAccessible(true);
            U = constructor.newInstance();
        } catch (Exception ignored) {
            throw new RuntimeException("Class initialization failed: Unsafe initialization failed");
        }
    }

    public ConcurrentArrayList(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Illegal Capacity: " +
                    initialCapacity);
        }
        BASE = U.arrayBaseOffset(Object[].class);
        /*通过首元素地址加上索引乘以scale(每个元素引用占位)可以得到元素位置，由于每个元素
        引用大小肯定是2的次方(4或8)，所以乘操作可以用位移操作来代替，aShift就是要左移的位数*/
        ASHIFT = 31 - Integer.numberOfLeadingZeros(U.arrayIndexScale(Object[].class));
        this.elementData = new Object[initialCapacity];
    }

    public ConcurrentArrayList() {
        BASE = U.arrayBaseOffset(Object[].class);
        /*通过首元素地址加上索引乘以scale(每个元素引用占位)可以得到元素位置，由于每个元素
        引用大小肯定是2的次方(4或8)，所以乘操作可以用位移操作来代替，aShift就是要左移的位数*/
        ASHIFT = 31 - Integer.numberOfLeadingZeros(U.arrayIndexScale(Object[].class));
        this.elementData = new Object[10];
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Object o) {
        Object[] elementDataView;
        synchronized (this) {
            elementDataView = new Object[size];
            System.arraycopy(elementData, 0, elementDataView, 0, size);
        }
        if (o == null) {
            for (Object value : elementDataView) {
                if (null == value) {
                    return true;
                }
            }
        } else {
            for (Object value : elementDataView) {
                if (o.equals(value)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        return new Itr();
    }

    @Override
    public Object[] toArray() {
        return this.elementData;
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized boolean add(E e) {
        ensureCapacityInternal(size + 1);
        elementData[size++] = e;
        return true;
    }

    private void ensureCapacityInternal(int minCapacity) {
        ensureExplicitCapacity(minCapacity);
    }


    private void ensureExplicitCapacity(int minCapacity) {
        if (minCapacity - elementData.length > 0) {
            grow(minCapacity);
        }
    }

    @Override
    public synchronized boolean remove(Object o) {
        if (o == null) {
            for (int index = 0; index < size; index++) {
                if (elementData[index] == null) {
                    fastRemove(index);
                    return true;
                }
            }
        } else {
            for (int index = 0; index < size; index++) {
                if (o.equals(elementData[index])) {
                    fastRemove(index);
                    return true;
                }
            }
        }
        return false;
    }

    private void grow(int minCapacity) {
        Object[] data = this.elementData;
        int oldCapacity = data.length;
        int newCapacity = oldCapacity << 1;
        if (newCapacity < minCapacity) {
            newCapacity = minCapacity;
        }
        if (newCapacity > MAX_ARRAY_SIZE) {
            newCapacity = MAX_ARRAY_SIZE;
        }
        this.elementData = Arrays.copyOf(data, newCapacity);
    }

    private synchronized void fastRemove(int index) {
        int numMoved = size - index - 1;
        if (numMoved > 0) {
            System.arraycopy(elementData, index + 1, elementData, index,
                    numMoved);
        }
        elementData[--size] = null;
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(int index, Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void clear() {
        this.elementData = new Object[10];
    }

    @Override
    public E get(int index) {
        if (index > size - 1) {
            throw new IndexOutOfBoundsException();
        }
        return tabAt(index);
    }


    private E tabAt(int i) {
        return (E) U.getObjectVolatile(elementData, ((long) i << ASHIFT) + BASE);
    }

    @Override
    public synchronized E remove(int index) {
        rangeCheck(index);

        E oldValue = (E) elementData[index];

        int numMoved = size - index - 1;
        if (numMoved > 0) {
            System.arraycopy(elementData, index + 1, elementData, index,
                    numMoved);
        }
        elementData[--size] = null;
        return oldValue;
    }

    private void rangeCheck(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
        }
    }

    private String outOfBoundsMsg(int index) {
        return "Index: " + index + ", Size: " + size;
    }

    @Override
    public int indexOf(Object o) {
        Object[] elementDataView;
        {
            synchronized (this) {
                elementDataView = new Object[size];
                System.arraycopy(elementData, 0, elementDataView, 0, size);
            }
        }
        if (o == null) {
            for (int i = 0; i < elementDataView.length; i++) {
                if (null == elementDataView[i]) {
                    return i;
                }
            }
        } else {
            for (int i = 0; i < elementDataView.length; i++) {
                if (o.equals(elementDataView[i])) {
                    return i;
                }
            }
        }
        return -1;
    }

    @Override
    public int lastIndexOf(Object o) {
        Object[] elementDataView;
        int index = -1;
        {
            synchronized (this) {
                elementDataView = new Object[size];
                System.arraycopy(elementData, 0, elementDataView, 0, size);
            }
        }
        if (o == null) {
            for (int i = 0; i < elementDataView.length; i++) {
                if (null == elementDataView[i]) {
                    index = i;
                }
            }
        } else {
            for (int i = 0; i < elementDataView.length; i++) {
                if (o.equals(elementDataView[i])) {
                    index = i;
                }
            }
        }
        return index;
    }

    @Override
    public ListIterator<E> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<E> listIterator(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<E> subList(int i, int i1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized E set(int index, Object element) {
        rangeCheck(index);
        E oldValue = (E) elementData[index];
        elementData[index] = element;
        return oldValue;
    }

    @Override
    public synchronized void add(int index, Object element) {
        rangeCheckForAdd(index);
        ensureCapacityInternal(size + 1);
        System.arraycopy(elementData, index, elementData, index + 1,
                size - index);
        elementData[index] = element;
        size++;
    }

    private void rangeCheckForAdd(int index) {
        if (index > size || index < 0) {
            throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
        }
    }

    private synchronized void writeObject(java.io.ObjectOutputStream s)
            throws java.io.IOException {

        for (int i = 0; i < size; i++) {
            s.writeObject(elementData[i]);
        }

    }

    /**
     * Reconstitute the <tt>ArrayList</tt> instance from a stream (that is,
     * deserialize it).
     */
    private synchronized void readObject(java.io.ObjectInputStream s)
            throws java.io.IOException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        Class cls = this.getClass();
        Field base = cls.getDeclaredField("BASE");
        base.setAccessible(true);
        base.set(this, U.arrayBaseOffset(Object[].class));
        Field aSft = cls.getDeclaredField("ASHIFT");
        aSft.setAccessible(true);
        aSft.set(this, 31 - Integer.numberOfLeadingZeros(U.arrayIndexScale(Object[].class)));
        this.elementData = new Object[10];
        for (; ; ) {
            try {
                this.add((E) s.readObject());
            } catch (Exception e) {
                break;
            }
        }
    }

    @Override
    public synchronized String toString() {
        StringBuilder stringBuilder = new StringBuilder(size << 1).append("[");
        int size = this.size;
        Object[] eleData = this.elementData;
        for (int i = 0; i < size; i++) {
            stringBuilder.append(eleData[i]).append(",");
        }
        stringBuilder.setCharAt(stringBuilder.length() - 1, ']');
        return stringBuilder.toString();
    }
}

