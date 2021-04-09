package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    public File f;
    public TupleDesc td;
    public int hfId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        this.hfId = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.hfId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        HeapPage ret = null;

        try {
            RandomAccessFile file = new RandomAccessFile(f, "r");
            int offset = pageSize * pid.pageNumber();
            byte[] data = new byte[pageSize];
            file.read(data, offset, pageSize);
            ret = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ret;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil((double)f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        TransactionId tid;
        public Iterator<Tuple> tupleIterator = null;
        public int curPageNum = -1;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            curPageNum = 0;
            tupleIterator = ((HeapPage) Database.getBufferPool().
                    getPage(tid, new HeapPageId(getId(), curPageNum), Permissions.READ_ONLY)).iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null) return false;
            if (tupleIterator.hasNext()) return true;
            if (curPageNum < numPages() - 1) return true;
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tupleIterator == null) throw new NoSuchElementException("@HeapFileIterator::next(): Not opened.");
            if (tupleIterator.hasNext()) return tupleIterator.next();
            if (curPageNum >= numPages() - 1) throw new NoSuchElementException("@HeapFileIterator::next()");
            curPageNum += 1;
            tupleIterator = ((HeapPage) Database.getBufferPool().
                    getPage(tid, new HeapPageId(getId(), curPageNum), Permissions.READ_ONLY)).iterator();
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            curPageNum = -1;
            tupleIterator = null;
        }
    }

}

