package java.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;

/**
 * An unbounded lock-free queue based on linked fixed-size ring segments.
 *
 * <p>The queue orders elements in FIFO (first-in-first-out) order and does not
 * permit {@code null} elements. Enqueue and dequeue operations rely on atomic
 * compare-and-set updates over segment metadata and slot contents and do not
 * block.
 *
 * <p>This implementation links together bounded ring segments. Producers append
 * into the current tail segment and consumers remove from the current head
 * segment. When a segment becomes full, a new segment is linked at the tail.
 *
 * <p>Iterators and spliterators traverse a snapshot captured when they are
 * created.
 *
 * <p>Beware that, unlike in most collections, the {@code size} method is
 * <em>not</em> a constant-time operation. Determining the current number of
 * elements requires a traversal of the queue and may report inexact results if
 * the queue is modified during traversal.
 *
 * @param <E> the type of elements held in this queue
 */
public final class LinkedConcurrentRingQueue<E> extends AbstractQueue<E> {
    /*
     * The queue is represented as a linked list of fixed-size segments. Slots
     * start as EMPTY, become populated by producers, and are later marked
     * SKIPPED once consumed or bypassed so that progress can continue under
     * contention.
     */
    private static final Object EMPTY = new Object();
    private static final Object SKIPPED = new Object();

    private static final VarHandle HEAD_SEG;
    private static final VarHandle TAIL_SEG;
    private static final VarHandle SEG_NEXT;
    private static final VarHandle SEG_HEAD;
    private static final VarHandle SEG_TAIL;
    private static final VarHandle SEG_CLOSED;
    private static final VarHandle ARRAY;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            HEAD_SEG = l.findVarHandle(LinkedConcurrentRingQueue.class, "headSeg", Segment.class);
            TAIL_SEG = l.findVarHandle(LinkedConcurrentRingQueue.class, "tailSeg", Segment.class);
            SEG_NEXT = l.findVarHandle(Segment.class, "next", Segment.class);
            SEG_HEAD = l.findVarHandle(Segment.class, "head", long.class);
            SEG_TAIL = l.findVarHandle(Segment.class, "tail", long.class);
            SEG_CLOSED = l.findVarHandle(Segment.class, "closed", boolean.class);
            ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final int segmentCapacity;

    private volatile Segment<E> headSeg;
    private volatile Segment<E> tailSeg;

    public LinkedConcurrentRingQueue() {
        this(1024);
    }

    public LinkedConcurrentRingQueue(int segmentCapacity) {
        if (segmentCapacity < 2) {
            throw new IllegalArgumentException("segmentCapacity >= 2");
        }
        this.segmentCapacity = roundPow2(segmentCapacity);

        Segment<E> first = new Segment<>(this.segmentCapacity);
        this.headSeg = first;
        this.tailSeg = first;
    }

    @Override
    public boolean offer(E e) {
        Objects.requireNonNull(e);

        for (;;) {
            Segment<E> t = (Segment<E>) TAIL_SEG.getAcquire(this);

            if (t.offer(e)) {
                return true;
            }

            Segment<E> next = (Segment<E>) SEG_NEXT.getAcquire(t);

            if (next == null) {
                Segment<E> n = new Segment<>(segmentCapacity);

                if (SEG_NEXT.compareAndSet(t, null, n)) {
                    TAIL_SEG.compareAndSet(this, t, n);
                }
            } else {
                TAIL_SEG.compareAndSet(this, t, next);
            }

            Thread.onSpinWait();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public E poll() {
        for (;;) {
            Segment<E> h = (Segment<E>) HEAD_SEG.getAcquire(this);

            Object r = h.poll();

            if (r != Segment.SEGMENT_EMPTY) {
                return (E) r;
            }

            Segment<E> next = (Segment<E>) SEG_NEXT.getAcquire(h);

            if (next == null) {
                return null;
            }

            HEAD_SEG.compareAndSet(this, h, next);
            Thread.onSpinWait();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public E peek() {
        Segment<E> s = (Segment<E>) HEAD_SEG.getAcquire(this);

        while (s != null) {
            Object r = s.peek();

            if (r != Segment.SEGMENT_EMPTY) {
                return (E) r;
            }

            s = (Segment<E>) SEG_NEXT.getAcquire(s);
        }

        return null;
    }

    @Override
    public boolean isEmpty() {
        return peek() == null;
    }

    @Override
    public int size() {
        int n = 0;

        for (E ignored : this) {
            if (++n == Integer.MAX_VALUE) {
                return n;
            }
        }

        return n;
    }

    @Override
    public boolean add(E e) {
        return offer(e);
    }

    @Override
    public boolean addAll(java.util.Collection<? extends E> c) {
        Objects.requireNonNull(c);
        if (c == this) {
            throw new IllegalArgumentException();
        }

        boolean modified = false;
        for (E e : c) {
            offer(e);
            modified = true;
        }
        return modified;
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) {
            return false;
        }

        Segment<E> s = (Segment<E>) HEAD_SEG.getAcquire(this);
        while (s != null) {
            if (s.contains(o)) {
                return true;
            }
            s = (Segment<E>) SEG_NEXT.getAcquire(s);
        }

        return false;
    }

    @Override
    public boolean remove(Object o) {
        if (o == null) {
            return false;
        }

        Segment<E> s = (Segment<E>) HEAD_SEG.getAcquire(this);
        while (s != null) {
            if (s.remove(o)) {
                return true;
            }
            s = (Segment<E>) SEG_NEXT.getAcquire(s);
        }

        return false;
    }

    @Override
    public java.util.Spliterator<E> spliterator() {
        return snapshot().spliterator();
    }

    @Override
    public Object[] toArray() {
        return snapshot().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return snapshot().toArray(a);
    }

    @Override
    public Iterator<E> iterator() {
        ArrayList<E> snapshot = new ArrayList<>();

        Segment<E> s = (Segment<E>) HEAD_SEG.getAcquire(this);

        while (s != null) {
            s.copyLive(snapshot);
            s = (Segment<E>) SEG_NEXT.getAcquire(s);
        }

        return snapshot.iterator();
    }

    private ArrayList<E> snapshot() {
        ArrayList<E> snapshot = new ArrayList<>();
        Segment<E> s = (Segment<E>) HEAD_SEG.getAcquire(this);

        while (s != null) {
            s.copyLive(snapshot);
            s = (Segment<E>) SEG_NEXT.getAcquire(s);
        }

        return snapshot;
    }

    private static final class Segment<E> {
        static final Object SEGMENT_EMPTY = new Object();

        final int capacity;
        final Object[] items;

        volatile long head;
        volatile long tail;
        volatile boolean closed;
        volatile Segment<E> next;

        Segment(int capacity) {
            this.capacity = capacity;
            this.items = new Object[capacity];

            for (int i = 0; i < capacity; i++) {
                items[i] = EMPTY;
            }
        }

        boolean offer(E e) {
            for (;;) {
                if ((boolean) SEG_CLOSED.getAcquire(this)) {
                    return false;
                }

                long t = (long) SEG_TAIL.getAndAdd(this, 1L);

                if (t >= capacity) {
                    SEG_CLOSED.compareAndSet(this, false, true);
                    return false;
                }

                int index = (int) t;

                if (ARRAY.compareAndSet(items, index, EMPTY, e)) {
                    return true;
                }

                Object cur = ARRAY.getAcquire(items, index);

                if (cur == SKIPPED) {
                    continue;
                }

                Thread.onSpinWait();
            }
        }

        Object poll() {
            for (;;) {
                long h0 = (long) SEG_HEAD.getAcquire(this);
                long t0 = (long) SEG_TAIL.getAcquire(this);

                if (h0 >= t0) {
                    return SEGMENT_EMPTY;
                }

                long h = (long) SEG_HEAD.getAndAdd(this, 1L);

                if (h >= capacity) {
                    return SEGMENT_EMPTY;
                }

                int index = (int) h;

                Object x = ARRAY.getAndSet(items, index, SKIPPED);

                if (x == EMPTY || x == SKIPPED) {
                    long t = (long) SEG_TAIL.getAcquire(this);

                    if (t <= h + 1) {
                        return SEGMENT_EMPTY;
                    }

                    continue;
                }

                return x;
            }
        }

        Object peek() {
            long h = (long) SEG_HEAD.getAcquire(this);
            long t = (long) SEG_TAIL.getAcquire(this);

            for (long i = h; i < t && i < capacity; i++) {
                Object x = ARRAY.getAcquire(items, (int) i);

                if (x != EMPTY && x != SKIPPED) {
                    return x;
                }
            }

            return SEGMENT_EMPTY;
        }

        boolean contains(Object o) {
            long h = (long) SEG_HEAD.getAcquire(this);
            long t = (long) SEG_TAIL.getAcquire(this);

            for (long i = h; i < t && i < capacity; i++) {
                Object x = ARRAY.getAcquire(items, (int) i);

                if (x != EMPTY && x != SKIPPED && o.equals(x)) {
                    return true;
                }
            }

            return false;
        }

        boolean remove(Object o) {
            long h = (long) SEG_HEAD.getAcquire(this);
            long t = (long) SEG_TAIL.getAcquire(this);

            for (long i = h; i < t && i < capacity; i++) {
                int index = (int) i;

                for (;;) {
                    Object x = ARRAY.getAcquire(items, index);

                    if (x == EMPTY || x == SKIPPED || !o.equals(x)) {
                        break;
                    }

                    if (ARRAY.compareAndSet(items, index, x, SKIPPED)) {
                        return true;
                    }

                    Thread.onSpinWait();
                }
            }

            return false;
        }

        @SuppressWarnings("unchecked")
        void copyLive(ArrayList<E> out) {
            long h = (long) SEG_HEAD.getAcquire(this);
            long t = (long) SEG_TAIL.getAcquire(this);

            for (long i = h; i < t && i < capacity; i++) {
                Object x = ARRAY.getAcquire(items, (int) i);

                if (x != EMPTY && x != SKIPPED) {
                    out.add((E) x);
                }
            }
        }
    }

    private static int roundPow2(int x) {
        int h = Integer.highestOneBit(x);
        return x == h ? x : h << 1;
    }
}
