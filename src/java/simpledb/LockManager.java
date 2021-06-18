package simpledb;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class LockManager {
    private Map<PageId, LockSemaphore> lockMap = new ConcurrentHashMap<>();
    private Map<TPidPair, LockStatus> lockStatusMap = new ConcurrentHashMap<>();

    private class LockSemaphore {
        private final Semaphore countSema = new Semaphore(1);
        private final Semaphore lockSema = new Semaphore(1);
        private final Semaphore upgradeSema = new Semaphore(1);
        private int readCount = 0;

        public void lockRead() {
            try {
                countSema.acquire();
                readCount++;
                if (readCount == 1) lockSema.acquire();
                countSema.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void lockWrite() {
            try {
                lockSema.acquire();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void upgrade(TransactionId tid) {
            try {
                countSema.acquire();
                readCount--;
                upgradeSema.acquire();
                if (readCount == 0) lockSema.release();
                countSema.release();
                lockSema.acquire();
                upgradeSema.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void unlockWrite() {
            lockSema.release();
        }

        public void unlockRead() {
            try {
                countSema.acquire();
                readCount--;
                if (readCount == 0) lockSema.release();
                countSema.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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

    class LockStatus {
        private final TransactionId tid;
        private final LockSemaphore lock;
        private Permissions perm;

        public LockStatus(TransactionId tid, LockSemaphore lock) {
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

        public void update(Permissions perm) {
            if (this.perm == null) {
                if (perm == Permissions.READ_ONLY) lock.lockRead();
                else {
                    lock.lockWrite();
                }
                this.perm = perm;
            } else if (this.perm == Permissions.READ_ONLY && perm == Permissions.READ_WRITE) {
                lock.upgrade(tid);
                this.perm = Permissions.READ_WRITE;
            }
        }

        public void unlock() {
            if (perm != null) {
                if (perm == Permissions.READ_ONLY) lock.unlockRead();
                else lock.unlockWrite();
                perm = null;
            }
        }
    }

    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) {
        LockSemaphore lock = lockMap.computeIfAbsent(pid, x -> new LockSemaphore());
        LockStatus status = lockStatusMap.computeIfAbsent(new TPidPair(tid, pid), x -> new LockStatus(tid, lock));
        status.update(perm);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return lockStatusMap.containsKey(new TPidPair(tid, pid));
    }

    public void releasePage(TransactionId tid, PageId pid) {
        LockStatus status = lockStatusMap.get(new TPidPair(tid, pid));
        if (status != null) {
            status.unlock();
            lockStatusMap.remove(new TPidPair(tid, pid));
        }
    }

    public Iterator<Map.Entry<TPidPair, LockStatus>> getLockStatusIterator() {
        return lockStatusMap.entrySet().iterator();
    }

}
