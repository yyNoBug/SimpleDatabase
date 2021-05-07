package simpledb;

import java.io.*;

import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    public final int numPages;
    public final LRUQue lruQue; // LRU policy.
    public final HashMap<PageId, Page> pageMap;

    private static class LRUQue {
        private final Node start, end;
        private final HashMap<PageId, Node> map;
        private final int numPages;

        public static class Node {
            public Node pre, post;
            public PageId pid;

            public Node() {   }

            public Node(Node pre, Node post, PageId pid) {
                this.pre = pre;
                this.post = post;
                this.pid = pid;
            }
        }

        public LRUQue(int numPages) {
            this.start = new Node();
            this.end = new Node();
            start.post = end;
            end.pre = start;
            this.map = new HashMap<>();
            this.numPages = numPages;
        }

        private void remove(PageId pid) {
            Node n = map.get(pid);
            n.pre.post = n.post;
            n.post.pre = n.pre;
            map.remove(pid);
        }

        private void insertLast(PageId pid) {
            Node n = new Node(end.pre, end, pid);
            end.pre.post = n;
            end.pre = n;
            map.put(pid, n);
        }

        public PageId evict() throws DbException {
            if (start.post == end) throw new DbException("@evict");
            PageId victim = start.post.pid;
            remove(victim);
            return victim;
        }

        public void refer(PageId pid) throws DbException {
            if (!map.containsKey(pid) && map.size() == numPages) throw new DbException("@refer");
            if (map.containsKey(pid)) remove(pid);
            insertLast(pid);
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.lruQue = new LRUQue(numPages);
        this.pageMap = new HashMap<>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        if (pageMap.containsKey(pid)) { // hit
            lruQue.refer(pid);
            Page page = pageMap.get(pid);
            page.markDirty(true, tid);
            return page;
        }
        // miss
        if (pageMap.size() == numPages) evictPage();
        lruQue.refer(pid);
        Page newPage = getPageByPid(pid);
        newPage.markDirty(perm == Permissions.READ_WRITE, tid);
        pageMap.put(pid, newPage);
        return newPage;
    }

    private Page getPageByPid(PageId pid) {
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        return f.readPage(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = f.insertTuple(tid, t);
        addPages(tid, dirtyPages);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> dirtyPages = f.deleteTuple(tid, t);
        addPages(tid, dirtyPages);
    }

    private synchronized void addPages(TransactionId tid, ArrayList<Page> dirtyPages) throws DbException {
        for (Page page: dirtyPages) {
            page.markDirty(true, tid);

            if (pageMap.containsKey(page.getId())) {
                lruQue.refer(page.getId());
                return;
            }
            if (pageMap.size() == numPages) evictPage();
            lruQue.refer(page.getId());
            pageMap.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid: pageMap.keySet()) flushPage(pid);
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        if (!pageMap.containsKey(pid)) return;
        try {
            if (pageMap.get(pid).isDirty() != null) flushPage(pid);
        } catch (IOException e) {
            e.printStackTrace();
        }
        pageMap.remove(pid);
        lruQue.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if (!pageMap.containsKey(pid)) return;
        if (pageMap.get(pid).isDirty() == null) return;
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page p = pageMap.get(pid);
        p.markDirty(false, null);
        f.writePage(p);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId victim = lruQue.evict();
        try {
            if (pageMap.get(victim).isDirty() != null) flushPage(victim);
        } catch (IOException e) {
            throw new DbException("@evictPage");
        }
        pageMap.remove(victim);
    }

}
