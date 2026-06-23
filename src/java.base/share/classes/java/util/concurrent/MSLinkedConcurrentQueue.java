package java.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * An unbounded lock-free queue based on the Michael and Scott linked-queue
 * algorithm.
 *
 * <p>The queue orders elements in FIFO (first-in-first-out) order and does not
 * permit {@code null} elements. Enqueue and dequeue operations rely on atomic
 * compare-and-set updates over linked nodes and do not block.
 *
 * <p>This implementation is intended as a compact linked-node queue variant
 * built around the core Michael-Scott algorithm in a garbage-collected
 * environment.
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
public final class MSLinkedConcurrentQueue<E> extends AbstractQueue<E> {
    /*
     * This is a straightforward linked-node realization of the Michael-Scott
     * queue. A dummy node separates head and tail movement, producers link new
     * nodes at the tail, and consumers advance the head toward the first live
     * node.
     */
    private static final VarHandle HEAD;
    private static final VarHandle TAIL;
    private static final VarHandle NEXT;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            HEAD = l.findVarHandle(MSLinkedConcurrentQueue.class, "head", Node.class);
            TAIL = l.findVarHandle(MSLinkedConcurrentQueue.class, "tail", Node.class);
            NEXT = l.findVarHandle(Node.class, "next", Node.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private volatile Node<E> head;
    private volatile Node<E> tail;

    public MSLinkedConcurrentQueue() {
        Node<E> dummy = new Node<>(null);
        head = dummy;
        tail = dummy;
    }

    @Override
    public boolean offer(E e) {
        Objects.requireNonNull(e);
        Node<E> node = new Node<>(e);

        for (;;) {
            Node<E> t = tail;
            Node<E> next = t.next;

            if (t == tail) {
                if (next == null) {
                    if (NEXT.compareAndSet(t, null, node)) {
                        TAIL.compareAndSet(this, t, node);
                        return true;
                    }
                } else {
                    TAIL.compareAndSet(this, t, next);
                }
            }

            Thread.onSpinWait();
        }
    }

    @Override
    public E poll() {
        for (;;) {
            Node<E> h = head;
            Node<E> t = tail;
            Node<E> next = h.next;

            if (h == head) {
                if (next == null) {
                    return null;
                }

                if (h == t) {
                    TAIL.compareAndSet(this, t, next);
                    continue;
                }

                E item = next.item;

                if (HEAD.compareAndSet(this, h, next)) {
                    h.next = h;
                    return item;
                }
            }

            Thread.onSpinWait();
        }
    }

    @Override
    public E peek() {
        for (;;) {
            Node<E> h = head;
            Node<E> next = h.next;

            if (h == head) {
                return next == null ? null : next.item;
            }

            Thread.onSpinWait();
        }
    }

    @Override
    public boolean isEmpty() {
        return peek() == null;
    }

    @Override
    public int size() {
        int count = 0;
        Node<E> p = head.next;

        while (p != null && count != Integer.MAX_VALUE) {
            if (p.item != null) {
                count++;
            }

            Node<E> next = p.next;
            if (next == p) {
                break;
            }

            p = next;
        }

        return count;
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

        Node<E> p = head.next;
        while (p != null) {
            E item = p.item;
            if (o.equals(item)) {
                return true;
            }

            Node<E> next = p.next;
            if (next == p) {
                break;
            }
            p = next;
        }

        return false;
    }

    @Override
    public boolean remove(Object o) {
        if (o == null) {
            return false;
        }

        restart:
        for (;;) {
            Node<E> pred = head;
            Node<E> p = pred.next;

            while (p != null) {
                Node<E> next = p.next;
                if (next == p) {
                    continue restart;
                }

                E item = p.item;
                if (o.equals(item)) {
                    if (NEXT.compareAndSet(pred, p, next)) {
                        if (p == tail) {
                            TAIL.compareAndSet(this, p, pred);
                        }
                        return true;
                    }
                    continue restart;
                }

                pred = p;
                p = next;
            }

            return false;
        }
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
        return new Iterator<>() {
            private Node<E> current = first();
            private E nextItem;

            private Node<E> first() {
                Node<E> p = head.next;
                while (p != null) {
                    E item = p.item;
                    if (item != null) {
                        nextItem = item;
                        return p;
                    }
                    p = p.next;
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public E next() {
                if (current == null) {
                    throw new NoSuchElementException();
                }

                E e = nextItem;
                Node<E> p = current.next;

                while (p != null) {
                    E item = p.item;
                    if (item != null) {
                        nextItem = item;
                        current = p;
                        return e;
                    }
                    p = p.next;
                }

                current = null;
                nextItem = null;
                return e;
            }
        };
    }

    private java.util.ArrayList<E> snapshot() {
        java.util.ArrayList<E> out = new java.util.ArrayList<>();
        Node<E> p = head.next;

        while (p != null) {
            E item = p.item;
            if (item != null) {
                out.add(item);
            }

            Node<E> next = p.next;
            if (next == p) {
                break;
            }
            p = next;
        }

        return out;
    }

    private static final class Node<E> {
        final E item;
        volatile Node<E> next;

        Node(E item) {
            this.item = item;
        }
    }
}
