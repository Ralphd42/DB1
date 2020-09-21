package storage;

import java.io.IOException;
 
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
 
import java.util.List;
import java.util.NoSuchElementException;
 

 

/**
 * A {@code BufferedFileManager} manages a storage space using the slotted page format and buffering.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class BufferedFileManager extends FileManager {
	List<SlottedPage> Buffer;
	int BufferSize;
	int TmpfileID=0;
	// TODO complete this class (10 points)

	/**
	 * Constructs a {@code BufferedFileManager}.
	 * 
	 * @param bufferSize
	 *            the number of {@code SlottedPage}s that the buffer can maintain
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public BufferedFileManager(int bufferSize) throws IOException {
		Buffer = new ArrayList<SlottedPage>();
		BufferSize = bufferSize;
	}
	/**
	 *  this finds the oldest item in the buffer and evicts it.  Item is returned to caller
	 * @return  the evicted SlottedPage
	 */
	SlottedPage CacheEviction()
	{
		SlottedPage Evicted =null;
		int i=0;
		LocalDateTime min = LocalDateTime.now();
		int mini =-1;
		for(; i< Buffer.size();i++)
		{
			if( Buffer.get(i).lastAccessed.compareTo(min)==-1)
			{	
				min = Buffer.get(i).lastAccessed;
				mini = i;
			}
		}
		Evicted = Buffer.get(mini);
		Buffer.remove(mini);
		return Evicted;
	}
	
	
	
	/**
	 *  Override of update.  puts most recently updated item into buffer.  Removes oldest
	 */
	@Override
	void updated(SlottedPage p, int fileID) throws IOException {
		p.Accessed();
		p.dirty =true;
		//1 is in buffer
		SlottedPage inBuf =Buffer.stream().filter(asd -> asd.pageID==p.pageID).findAny().orElse(null);
		if(inBuf==null)//check files system
		{
			// check if in file system
			SlottedPageFile f = file(fileID);
			if(f.get(p.pageID)==null)
			{
				f.save(p);
			}
			if(Buffer.size()<BufferSize	 )
			{
				Buffer.add(p);
			}else {
			SlottedPage evicted = CacheEviction();
			
			if(evicted.dirty)
			{
				//updated(evicted,0);
				SlottedPageFile fout = file(fileID);
				fout.save(evicted);
				evicted.dirty =false;
			}
			Buffer.add(p);
			}
		}
	}
	
	/**
	 * Returns an iterator over all objects stored in the the specified file.
	 * 
	 * @param fileID
	 *            the ID of the file
	 */
	@Override
	public Iterator<Object> iterator(int fileID) {
		
		return new BufferedFileIterator(fileID);
	}
	
	
	
	@Override
	SlottedPage page(int fileID, int pageID) throws IOException {
		// 1 find page in buffer
		fileID =TmpfileID;
		SlottedPage rv ;
		SlottedPage inBuf =Buffer.stream().filter(asd -> asd.pageID==pageID).findAny().orElse(null);
		if(inBuf!=null)
		{
			rv = inBuf;
		}
		else 
		{
			SlottedPageFile f = file(fileID);
			rv = f.get(pageID);
			// if buffer isn't full add
			rv.dirty =false;  // this is because it was just loaded.
			if(Buffer.size() <BufferSize)
			{
				Buffer.add(rv);
			}else
			{
				// do eviction
				SlottedPage evicted = CacheEviction();
				
				if(evicted.dirty)
				{
					//updated(evicted,0);
					SlottedPageFile fout = file(fileID);
					fout.save(evicted);
				}
				Buffer.add(rv);
			}
			
			
		}
		rv.Accessed();
		return rv;
	}
	class BufferedFileIterator implements Iterator<Object>{
		int maxpginBuffer()
		{
			int mx =-99;
			for( SlottedPage p : Buffer) 
			{
				if(p.pageID>mx)
				{
					mx =p.pageID;
					
				}
				
			}
			return mx;
			
			
			
		}
		
		
		
		int spSize=0;
		int currPg=0;
		int LastPageID;
		int FileID;
		SlottedPage currSP;
		Iterator <Object>spi;
		BufferedFileIterator(int FileID)
		{
			this.FileID = FileID;
			try 
			{
			 	spSize = size(FileID);				
				currSP = page(FileID,0);
				int mxbuf = maxpginBuffer();
				if( (spSize-1) >mxbuf)
				{
					LastPageID = spSize-1;
					
				}else
				{
					LastPageID = mxbuf;
				}
			
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			boolean retval = false;
			if( currSP ==null ) 
			{
				return false;
			}
			if( spi==null)
			{
				spi = currSP.iterator();
			}
			if(spi.hasNext()) 
			{
				retval =true;
			}
			else
			{				
				++currPg;
				if( currPg<=LastPageID)
				{
					try {
						currSP = page(FileID,currPg);
						spi = currSP.iterator();
						if(currSP!=null && spi!=null && spi.hasNext()) 
						{
							retval =true;
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						retval =false;
					}
					
				}
			}	
			return retval;
		}

		@Override
		public Object next() 
		{
			// TODO Auto-generated method stub
			 if(!hasNext())
			 {
				 throw new NoSuchElementException();
			 }
			 return spi.next();
		}
	}



}
