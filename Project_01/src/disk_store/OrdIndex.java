package disk_store;


import java.util.ArrayList;
import java.util.List;

/**
 * An ordered index.  Duplicate search key values are allowed,
 * but not duplicate index table entries.  In DB terminology, a
 * search key is not a superkey.
 *
 * A limitation of this class is that only single integer search
 * keys are supported.
 *
 */


public class OrdIndex implements DBIndex {

	private class Entry {
		int key;
		ArrayList<BlockCount> blocks;
	}

	private class BlockCount {
		int blockNo;
		int count;
	}

	ArrayList<Entry> entries;
	int size = 0;

	/**
	 * Create an new ordered index.
	 */
	public OrdIndex() {
		entries = new ArrayList<>();
	}

	@Override
	public List<Integer> lookup(int key) {
		// binary search of entries arraylist
		// return list of block numbers (no duplicates).
		// return if key not found, empty list

		List<Integer> result = new ArrayList<>();

		int l = entries.get(0).key;
		int r = entries.get(entries.size() - 1).key;

		while (l <= r) {
			int m = l + (r - l) / 2;

			// Check if key is present at mid
			if (entries.get(m).key == key){
				for(int i = 0; i < entries.get(m).blocks.size(); i++) {
					result.add(entries.get(m).blocks.get(i).blockNo);
				}
				break;
			}

			if (entries.get(m).key < key){
				l = m + 1;
			} else {
				r = m - 1;
			}
		}

		return result;
	}

	@Override
	public void insert(int key, int blockNum) {

		Entry e = new Entry();
		e.key = key;

		BlockCount b = new BlockCount();
		b.blockNo = blockNum;
		b.count = 1;

		e.blocks.add(b);

		int l = entries.get(0).key;
		int r = entries.get(entries.size() - 1).key;

		while (l <= r) {
			int m = l + (r - l) / 2;

			// Check if key is present at mid
			if (entries.get(m).key == key){
				return;
			}

			if(key > entries.get(m - 1).key && key < entries.get(m + 1).key){
				entries.add(m, e);
				return;
			}

			if (entries.get(m).key < key){
				l = m + 1;
			} else {
				r = m - 1;
			}
		}

		throw new UnsupportedOperationException();
	}

	@Override
	public void delete(int key, int blockNum) {
		// lookup key
		//  if key not found, should not occur.  Ignore it.
		//  decrement count for blockNum.
		//  if count is now 0, remove the blockNum.
		//  if there are no block number for this key, remove the key entry.
		throw new UnsupportedOperationException();
	}

	/**
	 * Return the number of entries in the index
	 * @return
	 */
	public int size() {
		return size;
		// you may find it useful to implement this

	}

	@Override
	public String toString() {
		throw new UnsupportedOperationException();
	}
}