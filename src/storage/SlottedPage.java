package storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Stack;

/**
 * A {@code SlottedPage} can store objects of possibly different sizes in a byte array.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class SlottedPage implements Iterable<Object> {

	/**
	 * The ID of this {@code SlottedPage}.
	 */
	int pageID;

	/**
	 * A byte array for storing the header of this {@code SlottedPage} and objects.
	 */
	byte[] data;

	/**
	 * The default size of {@code SlottedPage}s.
	 */
	static int SIZE = 4096 / 2;

	/**
	 * A flag indicating whether or not this {@code SlottedPage} has been updated and thus needs to be saved on disk.
	 */
	boolean dirty = true;

	/**
	 * Constructs a {@code SlottedPage}.
	 * 
	 * @param pageID
	 *            the ID of the {@code SlottedPage}
	 */
	public SlottedPage(int pageID) {
		data = new byte[SIZE];
		this.pageID = pageID;
		setEntryCount(0);
	}

	/**
	 * Returns the ID of this {@code SlottedPage}.
	 * 
	 * @return the ID of this {@code SlottedPage}
	 */
	public int pageID() {
		return pageID;
	}

	/**
	 * Returns the byte array of this {@code SlottedPage}.
	 * 
	 * @return the byte array of this {@code SlottedPage}
	 */
	public byte[] data() {
		return data;
	}

	@Override
	public String toString() {
		String s = "";
		for (Object o : this) {
			if (s.length() > 0)
				s += ", ";
			s += o;
		}
		return "(page ID: " + pageID + ", objects: [" + s + "])";
	}

	/**
	 * Adds the specified object in this {@code SlottedPage}.
	 * 
	 * @param o
	 *            an object to add
	 * @return the index of the object in this {@code SlottedPage}
	 * @throws IOException
	 *             if an I/O error occurs
	 * @throws OverflowException
	 *             if this {@code SlottedPage} cannot accommodate the specified object
	 */
	public int add(Object o) throws IOException, OverflowException {
		int retval = 0;
		int cnt   = entryCount();
		retval = save(o);
		++cnt;
		setEntryCount(cnt);
		saveLocation(cnt-1,retval);
		return retval;
	}

	/**
	 * Returns the object at the specified index in this {@code SlottedPage} ({@code null} if that object was removed
	 * from this {@code SlottedPage}).
	 * 
	 * @param index
	 *            an index
	 * @return the object at the specified index in this {@code SlottedPage}; {@code null} if that object was removed
	 *         from this {@code SlottedPage}
	 * @throws IndexOutOfBoundsException
	 *             if an invalid index is given
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public Object get(int index) throws IndexOutOfBoundsException, IOException {
		// TODO complete this method (15 points)
		Object retval = null;
		int loc =getLocation( index);
		if(loc>0)
		{
			retval =toObject( data,loc);
		}
		return retval;
	}

	/**
	 * Puts the specified object at the specified index in this {@code SlottedPage}.
	 * 
	 * @param index
	 *            an index
	 * @param o
	 *            an object to add
	 * @return the object stored previously at the specified location in this {@code SlottedPage}; {@code null} if no
	 *         such object
	 * @throws IOException
	 *             if an I/O error occurs
	 * @throws OverflowException
	 *             if this {@code SlottedPage} cannot accommodate the specified object
	 * @throws IndexOutOfBoundsException
	 *             if an invalid index is used
	 */
	public Object put(int index, Object o) throws IOException, OverflowException, IndexOutOfBoundsException {
		if (index == entryCount()) {
			add(o);
			return null;
		}
		Object old = remove(index);
		saveLocation(index, save(o));
		return old;
	}

	/**
	 * Removes the object at the specified index from this {@code SlottedPage}.
	 * 
	 * @param index
	 *            an index within this {@code SlottedPage}
	 * @return the object stored previously at the specified location in this {@code SlottedPage}; {@code null} if no
	 *         such object
	 * @throws IndexOutOfBoundsException
	 *             if an invalid index is used
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public Object remove(int index) throws IndexOutOfBoundsException, IOException {
		Object retval = null;
		retval = get( index);
		saveLocation(index,-1);
		return retval;
	}

	/**
	 * Returns an iterator over all objects stored in this {@code SlottedPage}.
	 */
	@Override
	public Iterator<Object> iterator() {
		// TODO complete this method (10 points)
		return new SlottedPageIterator(this);
	}

	/**
	 * Reorganizes this {@code SlottedPage} to maximize its free space.
	 * 
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	protected void compact() throws IOException {
		int count = entryCount();
		
		Object [] objs = new Object[count];
		for (int i = 0; i < count; i++) 
		{
			int location = getLocation(i);
			if (location != -1) 
			{
				// get the object and store the object
				try 
				{
					objs[i] =get(i);
				}
				catch(IndexOutOfBoundsException iex)
				{
					// this should never happen
					objs[i]=null;
				}
			}
			else
			{
				objs[i]=null;
			}
			saveLocation(i,-1);
		}
		// put eveything back
		for (int i = 0; i < count; i++) 
		{
			if (objs[i] ==null)
			{
				saveLocation(i,-1);
			}
			else
			{
				byte[] b = toByteArray(objs[i]);
				int loc = endOfFreeSpace() - b.length;
				System.arraycopy(b, 0, data, loc, b.length);
				saveLocation(i,loc);
			}
		}
	}

	/**
	 * Saves the specified object in the free space of this {@code SlottedPage}.
	 * 
	 * @param o
	 *            an object
	 * @return the location at which the object is saved within this {@code SlottedPage}
	 * @throws OverflowException
	 *             if this {@code SlottedPage} cannot accommodate the specified object
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	protected int save(Object o) throws OverflowException, IOException {
		byte[] b = toByteArray(o);
		if (freeSpaceSize() < b.length + Integer.BYTES) {
			compact();
			if (freeSpaceSize() < b.length + Integer.BYTES)
				throw new OverflowException();
		}
		int location = endOfFreeSpace() - b.length;
		System.arraycopy(b, 0, data, location, b.length);
		return location;

	}

	/**
	 * Returns the number of entries in this {@code SlottedPage}.
	 * 
	 * @return the number of entries in this {@code SlottedPage}
	 */
	protected int entryCount() {
		return readInt(0);
	}

	/**
	 * Sets the number of entries in this {@code SlottedPage}.
	 * 
	 * @param count
	 *            the number of entries in this {@code SlottedPage}
	 */
	protected void setEntryCount(int count) {
		writeInt(0, count);
	}

	/**
	 * Returns the start location of the specified object within this {@code SlottedPage}.
	 * 
	 * @param index
	 *            an index that specifies an object.
	 * @return the start location of the specified object within this {@code SlottedPage}
	 */
	protected int getLocation(int index) {
		return readInt((index + 1) * Integer.BYTES);
	}

	/**
	 * Saves the start location of the specified object within this {@code SlottedPage}.
	 * 
	 * @param index
	 *            an index that specifies an object.
	 * @param location
	 *            a location within this {@code SlottedPage}
	 */
	protected void saveLocation(int index, int location) {
		writeInt((index + 1) * Integer.BYTES, location);
	}

	/**
	 * Returns the size of free space in this {@code SlottedPage}.
	 * 
	 * @return the size of free space in this {@code SlottedPage}
	 */
	public int freeSpaceSize() {
		return endOfFreeSpace() - headerSize();
	}

	/**
	 * Returns the size of the header in this {@code SlottedPage}.
	 * 
	 * @return the size of the header in this {@code SlottedPage}
	 */
	protected int headerSize() {
		return Integer.BYTES * (entryCount() + 1);
	}

	/**
	 * Returns the end location of the free space in this {@code SlottedPage}.
	 * 
	 * @return the end location of the free space in this {@code SlottedPage}
	 */
	protected int endOfFreeSpace() {
		int end = SIZE;
		int count = entryCount();
		for (int i = 0; i < count; i++) {
			int location = getLocation(i);
			if (location != -1) {
				end = Math.min(end, location);
			}
		}
		return end;
	}

	/**
	 * Writes an integer value at the specified location in the byte array of this {@code SlottedPage}.
	 * 
	 * @param location
	 *            a location in the byte array of this {@code SlottedPage}
	 * @param value
	 *            the value to write
	 */
	protected void writeInt(int location, int value) {
		data[location] = (byte) (value >>> 24);
		data[location + 1] = (byte) (value >>> 16);
		data[location + 2] = (byte) (value >>> 8);
		data[location + 3] = (byte) value;
	}

	/**
	 * Reads an integer at the specified location in the byte array of this {@code SlottedPage}.
	 * 
	 * @param location
	 *            a location in the byte array of this {@code SlottedPage}
	 * @return an integer read at the specified location in the byte array of this {@code SlottedPage}
	 */
	protected int readInt(int location) {
		return ((data[location]) << 24) + ((data[location + 1] & 0xFF) << 16) + ((data[location + 2] & 0xFF) << 8)
				+ (data[location + 3] & 0xFF);
	}

	/**
	 * Returns a byte array representing the specified object.
	 * 
	 * @param o
	 *            an object.
	 * @return a byte array representing the specified object
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	protected byte[] toByteArray(Object o) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(b);
		out.writeObject(o);
		out.flush();
		return b.toByteArray();
	}

	/**
	 * Returns an object created from the specified byte array.
	 * 
	 * @param b
	 *            a byte array
	 * @param offset
	 *            the offset in the byte array of the first byte to read
	 * @return an object created from the specified byte array
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	protected Object toObject(byte[] b, int offset) throws IOException {
		try {
			if (b == null)
				return null;
			return new ObjectInputStream(new ByteArrayInputStream(b, offset, b.length - offset)).readObject();
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	/**
	 * A {@code OverflowException} is thrown if a {@code SlottedPage} cannot accommodate an additional object.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public class OverflowException extends Exception {

		/**
		 * Automatically generated serial version UID.
		 */
		private static final long serialVersionUID = -3007432568764672956L;

	}

	/**
	 * An {@code IndexOutofBoundsException} is thrown if an invalid index is used.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public class IndexOutOfBoundsException extends Exception {

		/**
		 * Automatically generated serial version UID.
		 */
		private static final long serialVersionUID = 7167791498344223410L;

	}
	class SlottedPageIterator implements Iterator<Object>{
		private SlottedPage pg;
		private Stack <Object> stack;
		SlottedPageIterator(SlottedPage sp)
		{
			pg =sp;
			stack = new Stack<>();
			fillStack();
			
		}
		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return !stack.isEmpty();
		}

		@Override
		public Object next() 
		{
			// TODO Auto-generated method stub
			 if(!hasNext())
			 {
				 throw new NoSuchElementException();
				 
			 }
			 return stack.pop();
		}
		private void fillStack( ) //throws IndexOutOfBoundsException, IOException
		{
			int count = pg.entryCount();
			for (int i = 0; i < count; i++) {
				int location = pg.getLocation(i);
				if (location != -1) {
					try 
					{
						Object itm =pg.get(i);
						if(itm!=null)
						{
							stack.push(itm);
						}
					}
					catch (Exception exp)
					{
						// iO exception 
						System.out.print(exp.toString());
					}
				}
			}
		}
	}

}
