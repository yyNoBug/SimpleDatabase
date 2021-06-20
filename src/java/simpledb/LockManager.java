package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class LockManager {
    private Map<PageId, LockSemaphore> lockMap = new ConcurrentHashMap<>();
    private Map<TPidPair, LockingInfo> lockStatusMap = new ConcurrentHashMap<>();
    private DependencyGraph graph = new DependencyGraph();

    private class DependencyGraph {
        private class LockPerm {
            public LockSemaphore lock;
            public Permissions perm;

            public LockPerm(LockSemaphore lock, Permissions perm) {
                this.lock = lock;
                this.perm = perm;
            }

            public LockSemaphore getLock() {
                return lock;
            }
        }

        private class TransPerm {
            public TransactionId tid;
            public Permissions perm;

            public TransPerm(TransactionId tid, Permissions perm) {
                this.tid = tid;
                this.perm = perm;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                TransPerm that = (TransPerm) o;
                return Objects.equals(tid, that.tid) &&
                        Objects.equals(perm, that.perm);
            }

            @Override
            public int hashCode() {
                return Objects.hash(tid, perm);
            }
        }

        private Map<TransactionId, LockPerm> lockPermMap = new ConcurrentHashMap<>();
        private Map<LockSemaphore, Set<TransPerm>> transPermSetMap = new ConcurrentHashMap<>();

        private boolean checkCycle(TransactionId start, TransactionId cur) {
            LockPerm lp = lockPermMap.get(cur);
            if (lp == null) return false;
            for (TransPerm tp: transPermSetMap.getOrDefault(lp.getLock(), new HashSet<>())) {
                if (tp.perm == Permissions.READ_ONLY && lp.perm == Permissions.READ_ONLY) continue;
                if (tp.tid == cur) continue;
                if (tp.tid == start) return true;
                if (checkCycle(start, tp.tid)) return true;
            }
            return false;
        }

        public synchronized void acquire(TransactionId tid, LockSemaphore lock, Permissions perm) {
            lockPermMap.remove(tid);
            transPermSetMap.computeIfAbsent(lock, x -> new HashSet<>()).add(new TransPerm(tid, perm));
        }

        public synchronized void release(TransactionId tid, LockSemaphore lock, Permissions perm) {
            Set<TransPerm> set = transPermSetMap.get(lock);
            set.remove(new TransPerm(tid, perm));
            if (set.isEmpty()) transPermSetMap.remove(lock);
        }

        public synchronized boolean check(TransactionId tid, LockSemaphore lock, Permissions perm) {
            lockPermMap.put(tid, new LockPerm(lock, perm));
            if (checkCycle(tid, tid)) {
                lockPermMap.remove(tid);
                return false;
            }
            return true;
        }
    }

    private class LockSemaphore {
        private final Semaphore countSema = new Semaphore(1);
        private final Semaphore lockSema = new Semaphore(1);
        private final Semaphore upgradeSema = new Semaphore(1);
        private int readCount = 0;

        public void lockRead(TransactionId tid) throws TransactionAbortedException {
            try {
                if (!graph.check(tid, this, Permissions.READ_ONLY)) throw new TransactionAbortedException();
                countSema.acquire();
                readCount++;
                if (readCount == 1) lockSema.acquire();
                countSema.release();
                graph.acquire(tid, this, Permissions.READ_ONLY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void lockWrite(TransactionId tid) throws TransactionAbortedException {
            try {
                if (!graph.check(tid, this, Permissions.READ_WRITE)) throw new TransactionAbortedException();
                lockSema.acquire();
                graph.acquire(tid, this, Permissions.READ_WRITE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void upgrade(TransactionId tid) throws TransactionAbortedException {
            try {
                if (!graph.check(tid, this, Permissions.READ_WRITE)) throw new TransactionAbortedException();
                countSema.acquire();
                readCount--;
                upgradeSema.acquire();
                if (readCount == 0) lockSema.release();
                countSema.release();
                lockSema.acquire();
                upgradeSema.release();
                graph.release(tid, this, Permissions.READ_ONLY);
                graph.acquire(tid, this, Permissions.READ_WRITE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void unlockRead(TransactionId tid) {
            try {
                countSema.acquire();
                readCount--;
                if (readCount == 0) lockSema.release();
                countSema.release();
                graph.release(tid, this, Permissions.READ_ONLY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void unlockWrite(TransactionId tid) {
            lockSema.release();
            graph.release(tid, this, Permissions.READ_WRITE);
        }
    }

    class TPidPair {
        TransactionId tid;
        PageId pid;

        public TPidPair(TransactionId tid, PageId pid) {
            this.pid = pid;
            this.tid = tid;
        }

        public TransactionId getTid() {
            return tid;
        }

        public PageId getPid() {
            return pid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TPidPair tPidPair = (TPidPair) o;
            return tid.equals(tPidPair.tid) &&
                    pid.equals(tPidPair.pid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tid, pid);
        }
    }

    class LockingInfo {
        private final TransactionId tid;
        private final LockSemaphore lock;
        private Permissions perm;

        public LockingInfo(TransactionId tid, LockSemaphore lock) {
            this.tid = tid;
            this.lock = lock;
            this.perm = null;
        }

        public TransactionId getTid() {
            return tid;
        }

        public LockSemaphore getLock() {
            return lock;
        }

        public Permissions getPerm() {
            return perm;
        }

        public void update(Permissions perm) throws TransactionAbortedException {
            if (this.perm == null) {
                if (perm == Permissions.READ_ONLY) lock.lockRead(tid);
                else {
                    lock.lockWrite(tid);
                }
                this.perm = perm;
            } else if (this.perm == Permissions.READ_ONLY && perm == Permissions.READ_WRITE) {
                lock.upgrade(tid);
                this.perm = Permissions.READ_WRITE;
            }
        }

        public void unlock() {
            if (perm != null) {
                if (perm == Permissions.READ_ONLY) lock.unlockRead(tid);
                else lock.unlockWrite(tid);
                perm = null;
            }
        }
    }



    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        LockSemaphore lock = lockMap.computeIfAbsent(pid, x -> new LockSemaphore());
        LockingInfo status = lockStatusMap.computeIfAbsent(new TPidPair(tid, pid), x -> new LockingInfo(tid, lock));
        status.update(perm);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return lockStatusMap.containsKey(new TPidPair(tid, pid));
    }

    public void releasePage(TransactionId tid, PageId pid) {
        LockingInfo status = lockStatusMap.get(new TPidPair(tid, pid));
        if (status != null) {
            status.unlock();
            lockStatusMap.remove(new TPidPair(tid, pid));
        }
    }

    public Iterator<Map.Entry<TPidPair, LockingInfo>> getLockStatusIterator() {
        return lockStatusMap.entrySet().iterator();
    }

}
