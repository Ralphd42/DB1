package storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * A {@code SlottedPageFile} represents a file consisting of {@code SlottedPage}s.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class SlottedPageFile {

	String name;

	/**
	 * A {@code RandomAccessFile}
	 */
	RandomAccessFile file;

	/**
	 * The number of seeks performed so far.
	 */
	int seeks = 0;

	/**
	 * The number of {@code SlottedPage}s read.
	 */
	int reads = 0;

	/**
	 * The number of {@code SlottedPage}s written.
	 */
	int writes = 0;

	/**
	 * Constructs a {@code SlottedPageFile}.
	 * 
	 * @param name
	 *            the system-dependent filename
	 * @throws FileNotFoundException
	 *             if the specified file cannot be found/created
	 */
	public SlottedPageFile(String name) throws FileNotFoundException {
		this.name = name;
		file = new java.io.RandomAccessFile(name, "rw");
	}

	@Override
	public String toString() {
		return "{name:" + name + ", reads:" + reads + ", writes:" + writes + "}";
	}

	/**
	 * Returns the number of {@code SlottedPage}s in this {@code SlottedPageFile}.
	 * 
	 * @return the number of {@code SlottedPage}s in this {@code SlottedPageFile}
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public int size() throws IOException {
		return (int) (file.length() / SlottedPage.SIZE);
	}

	/**
	 * Closes this {@code SlottedPageFile} and releases any system resources associated with this
	 * {@code SlottedPageFile}.
	 * 
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public void close() throws IOException {
		file.close();
	}

	/**
	 * Removes all data from this {@code SlottedPageFile}.
	 * 
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public void clear() throws IOException {
		file.close();
		new File(name).delete();
		file = new java.io.RandomAccessFile(name, "rw");
	}

	/**
	 * Creates a {@code SlottedPage} from this {@code SlottedPageFile} according to the specified page ID ({@code null}
	 * if no corresponding data is stored in this {@code SlottedPageFile}).
	 * 
	 * @param pageID
	 *            the ID of the {@code SlottedPage} to create
	 * @return a {@code SlottedPage} from this {@code SlottedPageFile}; {@code null} if no corresponding data is stored
	 *         in this {@code SlottedPageFile}
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public SlottedPage get(int pageID) throws IOException {
		if (pageID < 0)
			return null;
		long pos = ((long) pageID) * SlottedPage.SIZE;
		if (pos + SlottedPage.SIZE > file.length())
			return null;
		seek(pos);
		SlottedPage p = new SlottedPage(pageID);
		file.read(p.data());
		reads++;
		return p;
	}

	/**
	 * Saves the specified {@code SlottedPage} to this {@code SlottedPageFile}.
	 * 
	 * @param p
	 *            {@code SlottedPage}
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public void save(SlottedPage p) throws IOException {
		seek(p.pageID() * SlottedPage.SIZE);
		file.write(p.data());
		writes++;
	}

	/**
	 * Sets the file-pointer offset, measured from the beginning of this file, at which the next read or write occurs.
	 * 
	 * @param pos
	 *            the offset position, measured in bytes from the beginning of the file, at which to set the file
	 *            pointer.
	 * @throws IOException
	 *             if an I/O error occurs.
	 */
	void seek(long pos) throws IOException {
		if (pos != file.getFilePointer()) {
			file.seek(pos);
			seeks++;
		}
	}

}
