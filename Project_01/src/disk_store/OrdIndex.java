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

		// binary search
		int l = 0;
		int r = entries.size() - 1;

		while (l <= r) {
			int m = (int) Math.floor(l + (r - l) / 2);

			// if key is present at mid, add block numbers to result
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
		e.blocks = new ArrayList<BlockCount>();

		e.blocks.add(0, b);

		// if list is empty, insert first entry
		if(entries.isEmpty()){
			entries.add(0, e);
			entries.get(0).blocks.get(0).count++;
			return;
		}
		// if key is grater than all current keys, insert back
		if(entries.get(entries.size() - 1).key < key) {
			entries.add(e);
			return;
		}
		// if key is smaller than all entries, insert front
		if(entries.get(0).key > key){
			entries.add(e);
			return;
		}


		// Binary search
		int l = 0;
		int r = entries.size() - 1;

		while (l <= r) {
			int m = (int) Math.floor(l + (r - l) / 2);

			// if key exists, insert block number into existing blocks arrayList
			if (entries.get(m).key == key) {
				entries.get(m).blocks.add(b);
				int newBlockIdx = entries.get(m).blocks.size() - 1;
				entries.get(m).blocks.get(newBlockIdx).count++;
				return;
			}

			if (entries.get(m).key < key){
				l = m + 1;
			} else {
				r = m - 1;
			}
		}

		// if key doesnt exist, sorted insert
		for(int i = 1; i < entries.size() - 1; i++){
			if(entries.get(i - 1).key < key && entries.get(i + 1).key > key){
				entries.add(i, e);
				return;
			}
		}

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
		return entries.size();
		// you may find it useful to implement this

	}

	@Override
	public String toString() {
		throw new UnsupportedOperationException();
	}
}