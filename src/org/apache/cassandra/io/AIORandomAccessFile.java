/**
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

package org.apache.cassandra.io;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.concurrent.ContinuationContext;
import org.apache.cassandra.concurrent.ContinuationsExecutor;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.IContinuable;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.continuations.Suspendable;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.GuidGenerator;
import org.apache.cassandra.utils.LogUtil;
import org.apache.commons.javaflow.Continuation;
import org.apache.log4j.Logger;

/**
 * A <code>AIORandomAccessFile</code> is like a
 * <code>RandomAccessFile</code>, but it uses a private buffer so that most
 * operations do not require a disk access.
 * <P>
 * 
 * Note: The operations on this class are unmonitored. Also, the correct
 * functioning of the <code>RandomAccessFile</code> methods that are not
 * overridden here relies on the implementation of those methods in the
 * superclass.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public final class AIORandomAccessFile extends RandomAccessFile
{   
    private final static Logger logger_ = Logger.getLogger(AIORandomAccessFile.class);
    private final static ThreadLocal<Runnable> tls_ = new InheritableThreadLocal<Runnable>();    
    static final int LogBuffSz_ = 16; // 64K buffer
    public static final int BuffSz_ = (1 << LogBuffSz_);
    static final long BuffMask_ = ~(((long) BuffSz_) - 1L);
    /* Used to lock the creation of the disk thread pool instance */
    private static Lock createLock_ = new ReentrantLock(); 
    private static ExecutorService diskIOPool_;          
   
    /**
     * Submits a read request to the Kernel and is used
     * only when running in Continuations mode. The kernel
     * on read completion will resume the continuation passed
     * in to complete the read request.
     *  
     * @author alakshman
     *
     */    
    class AIOReader implements IContinuable
    {              
        /* the continuation that needs to be resumed on read completion */
        private ContinuationContext continuationCtx_;                
        
        AIOReader(ContinuationContext continuationCtx)
        {           
            continuationCtx_ = continuationCtx;            
        }
        
        public void run(Continuation c)
        {                     
            /* submit the read request */               
            continuationCtx_.setContinuation(c);
            ByteBuffer buffer = ByteBuffer.wrap( buffer_ );              
            fileChannel_.read(buffer, diskPos_, continuationCtx_, new ReadCompletionHandler());
        }
    }  
    
    /**
     * Read completion handler for AIO framework. The context
     * that is passed in, is a Continuation that needs to be
     * resumed on read completion.
     * 
     * @author alakshman
     *
     * @param <V> number of bytes read.
     */    
    class ReadCompletionHandler implements CompletionHandler<Integer, ContinuationContext>
    {                   
        public void cancelled(ContinuationContext attachment)
        {
        }
        
        public void completed(Integer result, ContinuationContext attachment)
        {    
            logger_.debug("Bytes read " + result);
            if ( attachment != null )
            {
                Continuation c = attachment.getContinuation();     
                attachment.result(result);
                if ( c != null )
                {                                           
                    c = Continuation.continueWith(c, attachment);
                    ContinuationsExecutor.doPostProcessing(c);
                }
            }            
        }
        
        public void failed(Throwable th, ContinuationContext attachment)
        {
        }
    }
    
    /*
     * This implementation is based on the buffer implementation in Modula-3's
     * "Rd", "Wr", "RdClass", and "WrClass" interfaces.
     */
    private boolean dirty_; // true iff unflushed bytes exist
    private boolean closed_; // true iff the file is closed
    private long curr_; // current position in file
    private long lo_, hi_; // bounds on characters in "buff"
    private byte[] buffer_ = new byte[0]; // local buffer
    private long maxHi_; // this.lo + this.buff.length
    private boolean hitEOF_; // buffer contains last file block?
    private long diskPos_; // disk position    
    private AsynchronousFileChannel fileChannel_; // asynchronous file channel used for AIO.
    private boolean bContinuations_; // indicates if used in continuations mode.    
    
    /*
     * To describe the above fields, we introduce the following abstractions for
     * the file "f":
     * 
     * len(f) the length of the file curr(f) the current position in the file
     * c(f) the abstract contents of the file disk(f) the contents of f's
     * backing disk file closed(f) true iff the file is closed
     * 
     * "curr(f)" is an index in the closed interval [0, len(f)]. "c(f)" is a
     * character sequence of length "len(f)". "c(f)" and "disk(f)" may differ if
     * "c(f)" contains unflushed writes not reflected in "disk(f)". The flush
     * operation has the effect of making "disk(f)" identical to "c(f)".
     * 
     * A file is said to be *valid* if the following conditions hold:
     * 
     * V1. The "closed" and "curr" fields are correct:
     * 
     * f.closed == closed(f) f.curr == curr(f)
     * 
     * V2. The current position is either contained in the buffer, or just past
     * the buffer:
     * 
     * f.lo <= f.curr <= f.hi
     * 
     * V3. Any (possibly) unflushed characters are stored in "f.buff":
     * 
     * (forall i in [f.lo, f.curr): c(f)[i] == f.buff[i - f.lo])
     * 
     * V4. For all characters not covered by V3, c(f) and disk(f) agree:
     * 
     * (forall i in [f.lo, len(f)): i not in [f.lo, f.curr) => c(f)[i] ==
     * disk(f)[i])
     * 
     * V5. "f.dirty" is true iff the buffer contains bytes that should be
     * flushed to the file; by V3 and V4, only part of the buffer can be dirty.
     * 
     * f.dirty == (exists i in [f.lo, f.curr): c(f)[i] != f.buff[i - f.lo])
     * 
     * V6. this.maxHi == this.lo + this.buff.length
     * 
     * Note that "f.buff" can be "null" in a valid file, since the range of
     * characters in V3 is empty when "f.lo == f.curr".
     * 
     * A file is said to be *ready* if the buffer contains the current position,
     * i.e., when:
     * 
     * R1. !f.closed && f.buff != null && f.lo <= f.curr && f.curr < f.hi
     * 
     * When a file is ready, reading or writing a single byte can be performed
     * by reading or writing the in-memory buffer without performing a disk
     * operation.
     */
    
    /**
     * Open a AIORandomAccessFile for r/w operations.
     * @param file file to be opened.
     */
    public AIORandomAccessFile(File file) throws IOException
    {
        super(file, "rw");
        this.init(file, 0, false);
    }
    
    /**
     * Open a AIORandomAccessFile for r/w operations.
     * @param file file to be opened.
     * @param bContinuations specify if continuations
     *        support is required.
     */
    public AIORandomAccessFile(File file, boolean bContinuations) throws IOException
    {
        super(file, "rw");
        this.init(file, 0, bContinuations);        
    }
    
    /**
     * Open a AIORandomAccessFile for r/w operations.
     * @param file file to be opened
     * @param size amount of data to be buffer as part 
     *        of r/w operations
     * @throws IOException
     */
    public AIORandomAccessFile(File file, int size) throws IOException
    {
        super(file, "rw");
        init(file, size, false);
    }
    
    /**
     * Open a AIORandomAccessFile for r/w operations.
     * @param file file to be opened
     * @param size amount of data to be buffer as part 
     *        of r/w operations
     * @param bContinuations specify if continuations
     *        support is required.
     * @throws IOException
     */
    public AIORandomAccessFile(File file, int size, boolean bContinuations) throws IOException
    {
        super(file, "rw");
        init(file, size, bContinuations);
    }
    
    /**
     * Open a AIORandomAccessFile for r/w operations.
     * @param name of file to be opened.     
     */
    public AIORandomAccessFile(String name) throws IOException
    {
        super(name, "rw");
        this.init(new File(name), 0, false);
    }
    
    /**
     * Open a AIORandomAccessFile for r/w operations.
     * @param name of file to be opened.
     * @param bContinuations specify if continuations
     *        support is required.
     */
    public AIORandomAccessFile(String name, boolean bContinuations) throws IOException
    {
        super(name, "rw");
        this.init(new File(name), 0, bContinuations);
    }
    
    /**
     * Open a AIORandomAccessFile for r/w operations.
     * @param name of file to be opened.
     * @param size buffering size to be used.
     */
    public AIORandomAccessFile(String name, int size) throws IOException
    {
        super(name, "rw");
        this.init(new File(name), size, false);
    }
    
    /**
     * Open a AIORandomAccessFile for r/w operations.
     * @param name of file to be opened.
     * @param name of file to be opened.
     * @param bContinuations specify if continuations
     *        support is required.
     */
    public AIORandomAccessFile(String name, int size, boolean bContinuations) throws IOException
    {
        super(name, "rw");
        this.init(new File(name), size, bContinuations);
    }
    
    private void init(File file, int size, boolean bVal) throws IOException
    {
        bContinuations_ = bVal;
        OpenOption[] openOptions = new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ};
        this.dirty_ = this.closed_ = false;
        this.lo_ = this.curr_ = this.hi_ = 0;
        this.buffer_ = (size > BuffSz_) ? new byte[size] : new byte[BuffSz_];
        this.maxHi_ = (long) BuffSz_;
        this.hitEOF_ = false;
        this.diskPos_ = 0L;        
        /* set up the asynchronous file channel */
        if ( diskIOPool_ == null )
        {
            createLock_.lock(); 
            try
            {
                if ( diskIOPool_ == null )
                {
                    int maxThreads = DatabaseDescriptor.getThreadsPerPool();
                    diskIOPool_ = new ContinuationsExecutor( maxThreads,
                            maxThreads,
                            Integer.MAX_VALUE,
                            TimeUnit.SECONDS,
                            new LinkedBlockingQueue<Runnable>(),
                            new ThreadFactoryImpl("DISK-IO-POOL")
                            ); 
                }                                            
            }
            finally
            {
                createLock_.unlock();
            }            
        }
        Set<OpenOption> set = new HashSet<OpenOption>( Arrays.asList(openOptions) );
        fileChannel_ = AsynchronousFileChannel.open(file.toPath(), set, diskIOPool_);        
    }
    
    public void close() throws IOException
    {
        this.onClose();
        this.closed_ = true;     
        fileChannel_.close();
    }
    
    /**
     * Flush any bytes in the file's buffer that have not yet been written to
     * disk. If the file was created read-only, this method is a no-op.
     */
    public void flush() throws IOException
    {
        this.flushBuffer();
    }
    
    /**
     * Flush any dirty bytes in the buffer to disk. 
    */
    private void flushBuffer() throws IOException
    {        
        if (this.dirty_)
        {            
            int len = (int) (this.curr_ - this.lo_);            
            doWrite(this.lo_, false);
            this.diskPos_ = this.curr_;
            this.dirty_ = false;
        }                
    }
    
    /**
     * Invoked when close() is invoked and causes the flush
     * of the last few bytes to block when the write is submitted.
     * @throws IOException
     */
    private void onClose() throws IOException
    {
        if (this.dirty_)
        {
            int len = (int) (this.curr_ - this.lo_);            
            doWrite(this.lo_, true);
            this.diskPos_ = this.curr_;
            this.dirty_ = false;
        }  
    }
    
    /**
     * This method submits an I/O for write where the write happens at
     * <i>position</i> within the file.
     * 
     * @param position to seek to within the file
     * @param onClose indicates if this method was invoked on a close().
     */
    private void doWrite(long position, boolean onClose)
    {          
        ByteBuffer buffer = ByteBuffer.wrap(buffer_);  
        int length = (int) (this.curr_ - this.lo_);
        buffer.limit(length);         
        Future<Integer> futurePtr = fileChannel_.write(buffer, position, null, new WriteCompletionHandler<Integer>());  
        if ( onClose )
        {
            try
            {
                /* this will block but will execute only on a close() */
                futurePtr.get();
            }
            catch (ExecutionException ex)
            {
                logger_.warn(LogUtil.throwableToString(ex));
            }
            catch (InterruptedException ex)
            {
                logger_.warn(LogUtil.throwableToString(ex));
            }
        }
        buffer_ = new byte[buffer_.length];
    }
    
    /**
     * Read at most "this.buff.length" bytes into "this.buff", returning the
     * number of bytes read. If the return result is less than
     * "this.buff.length", then EOF was read.
     */
    private int fillBuffer() throws IOException
    {        
        int cnt = 0;        
        ByteBuffer buffer = ByteBuffer.allocate(buffer_.length);                 
        Future<Integer> futurePtr = fileChannel_.read(buffer, this.diskPos_, null, new ReadCompletionHandler());       
                      
        try
        {
            /* 
             * This should block
            */
            cnt = futurePtr.get();
        }
        catch (ExecutionException ex)
        {
            logger_.warn(LogUtil.throwableToString(ex));
        }
        catch (InterruptedException ex)
        {
            logger_.warn(LogUtil.throwableToString(ex));
        }
        
        if ( (cnt < 0) && ( this.hitEOF_ = (cnt < this.buffer_.length) ) ) 
        {
            // make sure buffer that wasn't read is initialized with -1
            if ( cnt < 0 )
                cnt = 0;            
            Arrays.fill(buffer_, cnt, this.buffer_.length, (byte) 0xff);
        }
        else
        {            
            buffer_ = buffer.array();
        }
        this.diskPos_ += cnt;
        return cnt;
    } 
 
    /**
     * Read as much data as indicated by the size of the buffer.
     * This method is only invoked in continuation mode.
     */
    private int fillBuffer2()
    {    
        ContinuationContext continuationCtx = (ContinuationContext)Continuation.getContext();        
        IContinuable reader = new AIOReader( continuationCtx );
        ContinuationsExecutor.putInTls(reader);
        /* suspend the continuation */                 
        Continuation.suspend();
                                
        continuationCtx = (ContinuationContext)Continuation.getContext();                        
        int cnt = (Integer)continuationCtx.result();
        
        if ( (cnt < 0) && ( this.hitEOF_ = (cnt < this.buffer_.length) ) ) 
        {
            // make sure buffer that wasn't read is initialized with -1
            if ( cnt < 0 )
                cnt = 0;            
            Arrays.fill(buffer_, cnt, this.buffer_.length, (byte) 0xff);
        }           
        this.diskPos_ += cnt;
        return cnt;
    }
    
    /**
     * This method positions <code>this.curr</code> at position
     * <code>pos</code>. If <code>pos</code> does not fall in the current
     * buffer, it flushes the current buffer and loads the correct one.
     * <p>
     * 
     * On exit from this routine <code>this.curr == this.hi</code> iff
     * <code>pos</code> is at or past the end-of-file, which can only happen
     * if the file was opened in read-only mode.
     */
    public void seek(long pos) throws IOException
    {        
        if (pos >= this.hi_ || pos < this.lo_)
        {
            // seeking outside of current buffer -- flush and read
            this.flushBuffer();
            this.lo_ = pos & BuffMask_; // start at BuffSz boundary
            this.maxHi_ = this.lo_ + (long) this.buffer_.length;
            if (this.diskPos_ != this.lo_)
            {
                this.diskPos_ = this.lo_;
            }
            
            int n = 0;
            /* Perform the read operations in continuation style */
            if ( bContinuations_ )
            {                   
                n = fillBuffer2();
            }
            else
            {
                n = fillBuffer();
            }
            this.hi_ = this.lo_ + (long) n;
        }
        else
        {
            // seeking inside current buffer -- no read required
            if (pos < this.curr_)
            {
                // if seeking backwards, we must flush to maintain V4
                this.flushBuffer();
            }
        }
        this.curr_ = pos;
    }

    
    public long getFilePointer()
    {
        return this.curr_;
    }
    
    public long length() throws IOException
    {
        return Math.max(this.curr_, super.length());
    }
    
    public int read() throws IOException
    {
        if (this.curr_ >= this.hi_)
        {
            // test for EOF
            // if (this.hi < this.maxHi) return -1;
            if (this.hitEOF_)
                return -1;
            
            // slow path -- read another buffer
            this.seek(this.curr_);
            if (this.curr_ == this.hi_)
                return -1;
        }
        byte res = this.buffer_[(int) (this.curr_ - this.lo_)];
        this.curr_++;
        return ((int) res) & 0xFF; // convert byte -> int
    }
    
    public int read(byte[] b) throws IOException
    {
        return this.read(b, 0, b.length);
    }
    
    public int read(byte[] b, int off, int len) throws IOException
    {
        if (this.curr_ >= this.hi_)
        {
            // test for EOF
            // if (this.hi < this.maxHi) return -1;
            if (this.hitEOF_)
                return -1;
            
            // slow path -- read another buffer
            this.seek(this.curr_);
            if (this.curr_ == this.hi_)
                return -1;
        }
        len = Math.min(len, (int) (this.hi_ - this.curr_));
        int buffOff = (int) (this.curr_ - this.lo_);
        System.arraycopy(this.buffer_, buffOff, b, off, len);
        this.curr_ += len;
        return len;
    }
    
    public void write(int b) throws IOException
    {
        if (this.curr_ >= this.hi_)
        {
            if (this.hitEOF_ && this.hi_ < this.maxHi_)
            {
                // at EOF -- bump "hi"
                this.hi_++;
            }
            else
            {
                // slow path -- write current buffer; read next one
                this.seek(this.curr_);
                if (this.curr_ == this.hi_)
                {
                    // appending to EOF -- bump "hi"
                    this.hi_++;
                }
            }
        }
        this.buffer_[(int) (this.curr_ - this.lo_)] = (byte) b;
        this.curr_++;
        this.dirty_ = true;
    }
    
    public void write(byte[] b) throws IOException
    {
        this.write(b, 0, b.length);
    }
    
    public void write(byte[] b, int off, int len) throws IOException
    {
        while (len > 0)
        {
            int n = this.writeAtMost(b, off, len);
            off += n;
            len -= n;
        }
        this.dirty_ = true;
    }
    
    /*
     * Write at most "len" bytes to "b" starting at position "off", and return
     * the number of bytes written.
     */
    private int writeAtMost(byte[] b, int off, int len) throws IOException
    {
        if (this.curr_ >= this.hi_)
        {
            if (this.hitEOF_ && this.hi_ < this.maxHi_)
            {
                // at EOF -- bump "hi"
                this.hi_ = this.maxHi_;
            }
            else
            {
                // slow path -- write current buffer; read next one
                this.seek(this.curr_);
                if (this.curr_ == this.hi_)
                {
                    // appending to EOF -- bump "hi"
                    this.hi_ = this.maxHi_;
                }
            }
        }
        len = Math.min(len, (int) (this.hi_ - this.curr_));
        int buffOff = (int) (this.curr_ - this.lo_);
        System.arraycopy(b, off, this.buffer_, buffOff, len);
        this.curr_ += len;
        return len;
    }
}

class ReadImpl implements Runnable
{
    public void run()
    {
        int i = 0;
        try
        {
            System.out.println("About to start the whole thing ...");
            AIORandomAccessFile aRaf2 = new AIORandomAccessFile( new File("/var/cassandra/test.dat"), true );  
            System.out.println("About to seek ...");
            
            //aRaf2.seek(0L);                        
            System.out.println( aRaf2.readInt() );
            System.out.println( aRaf2.readUTF() );
            
            System.out.println("About to seek a second time ...");
            aRaf2.seek(66000L);
            System.out.println( aRaf2.readInt() );
            System.out.println( aRaf2.readUTF() );
            
            aRaf2.close();
        }
        catch( IOException ex )
        {
            ex.printStackTrace();
        }        
    }
}

class WriteImpl implements Runnable
{
    public void run()
    {
        int i = 0;
        try
        {
            AIORandomAccessFile aRaf2 = new AIORandomAccessFile( new File("/var/cassandra/test.dat"));                    
            while ( i < 10000 )
            {
                aRaf2.writeInt(32);
                aRaf2.writeUTF("Avinash Lakshman thinks John McCain is an idiot");
                ++i;
            }
            aRaf2.close();
        }
        catch( IOException ex )
        {
            ex.printStackTrace();
        }        
    }
}

/**
 * Write completion handler for AIO framework. The context
 * that is passed in, is a Continuation that needs to be
 * resumed on write completion. For now the continuation is
 * not used at all.
 * 
 * @author alakshman
 *
 * @param <V> number of bytes written.
 */
class WriteCompletionHandler<V> implements CompletionHandler<V, Continuation>
{
    private final static Logger logger_ = Logger.getLogger(WriteCompletionHandler.class);
    
    public void cancelled(Continuation attachment)
    {
    }
    
    public void completed(V result, Continuation attachment)
    {               
        logger_.debug("Bytes written " + result);
        while ( attachment != null )
        {
            attachment = Continuation.continueWith(attachment);
        }
    }
    
    public void failed(Throwable th, Continuation attachment)
    {
    }
}


