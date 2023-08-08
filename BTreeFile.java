/*
 * @(#) bt.java   98/03/24
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu).
 *
 */

/*
 *         CSE 4331/5331 B+ Tree Project (Spring 2023)
 *         Instructor: Abhishek Santra
 *
 */

package btree;

import java.io.*;

import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import btree.*;
import org.apache.log4j.Logger;

public class BTreeFile extends IndexFile implements GlobalConst {
	private static final Logger logger = Logger.getLogger(BTreeFile.class);

	private final static int MAGIC0 = 1989;

	private final static String lineSep = System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	public static void traceFilename(String filename) throws IOException {

		fos = new FileOutputStream(filename);
		trace = new DataOutputStream(fos);
	}

	public static void destroyTrace() throws IOException {
		if (trace != null)
			trace.close();
		if (fos != null)
			fos.close();
		fos = null;
		trace = null;
	}

	private BTreeHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;

	public BTreeHeaderPage getHeaderPage() {
		return headerPage;
	}

	private PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private Page pinPage(PageId pageno) throws PinPageException {
		try {
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
			return page;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}

	private void add_file_entry(String fileName, PageId pageno) throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}

	}

	private void delete_file_entry(String filename) throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	public BTreeFile(String filename) throws GetFileEntryException, PinPageException, ConstructPageException {

		headerPageId = get_file_entry(filename);

		headerPage = new BTreeHeaderPage(headerPageId);
		dbname = new String(filename);

	}

	public BTreeFile(String filename, int keytype, int keysize, int delete_fashion)
			throws GetFileEntryException, ConstructPageException, IOException, AddFileEntryException {

		headerPageId = get_file_entry(filename);
		if (headerPageId == null) // file not exist
		{
			headerPage = new BTreeHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keytype);
			headerPage.set_maxKeySize(keysize);
			headerPage.set_deleteFashion(delete_fashion);
			headerPage.setType(NodeType.BTHEAD);
		} else {
			headerPage = new BTreeHeaderPage(headerPageId);
		}

		dbname = new String(filename);

	}

	public void close()
			throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	public void destroyFile() throws IOException, IteratorException, UnpinPageException, FreePageException,
			DeleteFileEntryException, ConstructPageException, PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void _destroyFile(PageId pageno) throws IOException, IteratorException, PinPageException,
			ConstructPageException, UnpinPageException, FreePageException {

		BTSortedPage sortedPage;
		Page page = pinPage(pageno);
		sortedPage = new BTSortedPage(page, headerPage.get_keyType());

		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage(page, headerPage.get_keyType());
			RID rid = new RID();
			PageId childId;
			KeyDataEntry entry;
			for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) {
				childId = ((IndexData) (entry.data)).getData();
				_destroyFile(childId);
			}
		} else { // BTLeafPage

			unpinPage(pageno);
			freePage(pageno);
		}

	}

	private void updateHeader(PageId newRoot) throws IOException, PinPageException, UnpinPageException {

		BTreeHeaderPage header;
		PageId old_data;

		header = new BTreeHeaderPage(pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId(newRoot);
		unpinPage(headerPageId, true);

	}

	/**
	 * insert method inserts a new key and its associated record ID (RID) into the B+ tree. 
	 * It first checks if the tree is empty by examining the root page's ID. If the root is empty, 
	 * a new leaf page is created and the key and RID are inserted into it. If the root is not empty, 
	 * the insertKey method is called with the root page's ID.
	 */
	public void insert(KeyClass key, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
			NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
			LeafDeleteException, InsertException, IOException

	{
		// initially, no header page , create root node and point to invalid page =-1
		if (headerPage.get_rootId().pid == -1) {
			BTLeafPage root4mpagenew;
			// tree is empty, create first new page
			root4mpagenew = new BTLeafPage(headerPage.get_keyType());

			PageId pageId_newrootid, null_id = null;

			// page number of the current page is obtained through getCurPage()
			pageId_newrootid = root4mpagenew.getCurPage();

			pinPage(pageId_newrootid);
			// setting null value to next pointer
			root4mpagenew.setNextPage(new PageId(INVALID_PAGE));
			root4mpagenew.setPrevPage(new PageId(INVALID_PAGE));
			// Inserting record on the page that is created
			root4mpagenew.insertRecord(key, rid);
			// logger.info(key);

			// header page now points to the root page
			updateHeader(pageId_newrootid);
			// un-pinning the newRootPage as its marked as used when lower index page gets
			// split
			unpinPage(pageId_newrootid, true);
		} else {
			// Creating an instance of KeyDataEntry newRootEntry that will catch the return
			// statement from _insert(KeyClass, RID, pageId) method
			KeyDataEntry newRootEntry = null;
			newRootEntry = insertKey(key, rid, headerPage.get_rootId());

			/* Split occurs */
			// If the newRootEntry is not null means a spilt should occurs with new index
			// page
			if (newRootEntry != null) {
				IndexData data_coming = (IndexData) newRootEntry.data;
				// Creating a new index page as the leaf page spilt occur
				BTIndexPage IdxPg = new BTIndexPage(NodeType.INDEX);
				// Inserting record on this index page in the form of <key, pageId>;
				// newRootPage.insertKey( newRootEntry.key,
				// ((IndexData)newRootEntry.data).getData())
				IdxPg.insertKey(newRootEntry.key, data_coming.getData());
				// the old root is split and it will now become the left child of new root;
				// setting the prevPage pointer to the old root using headerPage.get_rootId()
				IdxPg.setPrevPage(headerPage.get_rootId());
				// UnPinning page the new root using its page id
				unpinPage(IdxPg.getCurPage(), true);
				// Update the header to new root using its page id
				updateHeader(IdxPg.getCurPage());
			}
		}
	}

	/**
	 * insertKey method examines the type of the page indicated by presentPageId (an index page or a leaf page) 
	 * and calls the appropriate method to insert the key and RID into the page. If a split occurs during the insertion, 
	 * a new page is created to hold the "overflow" records, and a key point to the new page is returned to be inserted 
	 * into the parent page.
	 */
	private KeyDataEntry insertKey(KeyClass key, RID rid, PageId presentPageId)
			throws PinPageException, IOException, ConstructPageException, LeafDeleteException, ConstructPageException,
			DeleteRecException, IndexSearchException, UnpinPageException, LeafInsertRecException, ConvertException,
			IteratorException, IndexInsertRecException, KeyNotMatchException, NodeNotMatchException, InsertException {
		// creating a BTSortedPage presentPage of the page which will associate the
		// sorted page instance with the page instance
		BTSortedPage presentPage = new BTSortedPage(presentPageId, headerPage.get_keyType());
		if (presentPage.getType() == NodeType.INDEX) {
			// When presentPage is of type Index
			return insertInIndex(key, rid, presentPageId);
		} else if (presentPage.getType() == NodeType.LEAF) {
			return insertInLeaf(key, rid, presentPageId);
		} else {
			logger.error("Insertion error!");
			throw new InsertException(null, "");
		}
	}

	/**
	 * The insertInLeaf method inserts the key and RID into a leaf page. If there is enough space in the page 
	 * for the new key and RID, they are simply inserted. If there is not enough space, the page is split into two pages, 
	 * and the new key and RID are inserted into the appropriate page. The method returns a key to be inserted into the 
	 * parent index page if a split occurred.
	 */
	private KeyDataEntry insertInLeaf(KeyClass key, RID rid, PageId presentPageId)
			throws PinPageException, IOException, ConstructPageException, LeafDeleteException, ConstructPageException,
			DeleteRecException, IndexSearchException, UnpinPageException, LeafInsertRecException, ConvertException,
			IteratorException, IndexInsertRecException, KeyNotMatchException, NodeNotMatchException, InsertException {
		// ------Creating present leaf page with page-id constructor
		// parameter-----------//
		BTLeafPage presentLeafPg = new BTLeafPage(presentPageId, headerPage.get_keyType());
		// Check if the presentLeafPg has space for new entries with
		// presentLeafPg.available_space() >= BT.getKeyDataLength( upEntry.key,
		// NodeType.LEAF)
		if (presentLeafPg.available_space() >= BT.getKeyDataLength(key, presentLeafPg.getType())) {
			// Space available so inserting record
			presentLeafPg.insertRecord(key, rid);
			unpinPage(presentLeafPg.getCurPage(), true);
			return null;
		} else {
			// Space not available so current page must be split into two pages.> leafpage
			// with id and setting the pointers,previous and next-one on it.
			BTLeafPage splitleaf = new BTLeafPage(headerPage.get_keyType());
			PageId splitleaf_Id = splitleaf.getCurPage();
			// Setting the next page pointer to the next page which was previously pointed
			// by old page
			splitleaf.setNextPage(presentLeafPg.getNextPage());
			// Setting old leaf next pointer to new leaf
			presentLeafPg.setNextPage(splitleaf_Id);
			splitleaf.setPrevPage(presentLeafPg.getCurPage());
			// creating temporary key data entry variables and RID to delete and from old
			// page to new page
			KeyDataEntry tempd, endoftmp = null;
			RID drid = new RID();
			System.out.println(presentLeafPg.getFirst(drid).data);

			int pointer = 0;
			for (tempd = presentLeafPg.getFirst(drid); tempd != null; tempd = presentLeafPg.getNext(drid)) {
				pointer++;
			}
			// logger.info("Number of records existing in old leaf = " + pointer);
			tempd = presentLeafPg.getFirst(drid);
			// Transferring the second half of data to another page through for loop
			for (int i = 1; i <= pointer; i++) {
				if (i > pointer / 2) {
					LeafData leaffulldata = (LeafData) tempd.data;
					System.out.println(leaffulldata);
					// Inserting it into the split page.
					splitleaf.insertRecord(tempd.key, leaffulldata.getData());
					// Copied page from old-leaf page is deleted
					presentLeafPg.deleteSortedRecord(drid);
					// fetch next record to be moved
					tempd = presentLeafPg.getCurrent(drid);
				} else {
					// the first half goes into the old page
					endoftmp = tempd;
					tempd = presentLeafPg.getNext(drid);
				}
			}
			// Comparision to send the record to respective page
			if (BT.keyCompare(key, endoftmp.key) > 0) {
				splitleaf.insertRecord(key, rid);
			} else {
				presentLeafPg.insertRecord(key, rid);
			}
			unpinPage(presentLeafPg.getCurPage(), true);
			// filling up the tmpEntry
			KeyDataEntry dataCopyUp;
			tempd = splitleaf.getFirst(drid);

			dataCopyUp = new KeyDataEntry(tempd.key, splitleaf_Id);
			unpinPage(splitleaf_Id, true);
			return dataCopyUp;
		}
	}

	/**
	 * The insertInIndex method inserts the key and RID into an index page by recursively calling _insert with 
	 * the appropriate child page ID.
	 */
	private KeyDataEntry insertInIndex(KeyClass key, RID rid, PageId presentPageId)
			throws PinPageException, IOException, ConstructPageException, LeafDeleteException, ConstructPageException,
			DeleteRecException, IndexSearchException, UnpinPageException, LeafInsertRecException, ConvertException,
			IteratorException, IndexInsertRecException, KeyNotMatchException, NodeNotMatchException, InsertException {
		// BTIndexPage presentidxPage associate the sorted page instance with the page
		// instance
		BTIndexPage presentidxPage = new BTIndexPage(presentPageId, headerPage.get_keyType());
		PageId next_Id = presentidxPage.getPageNoByKey(key);
		unpinPage(presentidxPage.getCurPage());
		KeyDataEntry shiftdataup = null;
		// Recursing the _insert() using shift data-up and passing correct parameters
		// then pin it again
		shiftdataup = insertKey(key, rid, next_Id);
		if (shiftdataup == null) {
			// if shift data-up is null no split occurs and no split occur, so null is
			// returned
			return null;
		} else {
			// Check if the currentIndexPage has space for new entries
			// currentIndexPage.available_space() >= BT.getKeyDataLength( upEntry.key,
			// NodeType.INDEX)
			if (presentidxPage.available_space() > BT.getKeyDataLength(shiftdataup.key, NodeType.INDEX)) {
				// Inserting the data in page as it has space
				IndexData data_coming = (IndexData) shiftdataup.data;
				presentidxPage.insertKey(shiftdataup.key, data_coming.getData());
				// un-pinning the page using pageId
				unpinPage(presentidxPage.getCurPage(), true);
			} else {
				// if no space is available, split has to be done , new page has to be created
				// after splitting,
				// Creating a BTIndepage currentIndexPage, a variable to store its pageId
				// CurrentIndexpageId a variable to store the pageId of the new key
				// nextPageId=currentIndexPage.getPageNoByKey(key)
				BTIndexPage newSplitIndex = new BTIndexPage(headerPage.get_keyType());
				KeyDataEntry tempd, templ = null;
				RID drid = new RID();
				// transfering datafrom currentIndexPage to newIndexPage
				for (tempd = presentidxPage.getFirst(drid); tempd != null; tempd = presentidxPage.getFirst(drid)) {
					// inserting into the second index page
					System.out.println(tempd.key);
					IndexData data_coming = (IndexData) tempd.data;
					// Inserting record in new index page
					newSplitIndex.insertKey(tempd.key, data_coming.getData());
					// Deleting record from current index page
					presentidxPage.deleteSortedRecord(drid);
				} // Make the split equal using other for loop to spilt the records equally
				for (tempd = newSplitIndex.getFirst(drid); newSplitIndex.available_space() < presentidxPage
						.available_space(); tempd = newSplitIndex.getFirst(drid)) {
					// inserting half records into first leaf back
					IndexData data_coming = (IndexData) (tempd.data);
					presentidxPage.insertKey(tempd.key, data_coming.getData());
					// removing from second index
					newSplitIndex.deleteSortedRecord(drid);
					templ = tempd;
				}
				tempd = newSplitIndex.getFirst(drid);
				// Compare the key using BT.keyCompare( upEntry.key, tmpEntry.key)
				if (BT.keyCompare(shiftdataup.key, tempd.key) > 0) {
					// the new key upEntry,key goes to the newIndexPage
					IndexData data_coming = (IndexData) (shiftdataup.data);
					newSplitIndex.insertKey(shiftdataup.key, data_coming.getData());
				} else {
					// else it goes on the currentIndex page
					IndexData data_coming = (IndexData) (shiftdataup.data);
					presentidxPage.insertKey(shiftdataup.key, data_coming.getData());
				}
				// unpinning currentIndexPage as it is dirty page
				unpinPage(presentidxPage.getCurPage(), true);
				shiftdataup = newSplitIndex.getFirst(drid);
				// Set the left link in the newIndexPage
				newSplitIndex.setPrevPage(((IndexData) shiftdataup.data).getData());
				// Delete the first record from newIndexPage
				newSplitIndex.deleteSortedRecord(drid);
				unpinPage(newSplitIndex.getCurPage(), true);
				// set the higher Index page in the hierarchy to point to thenewIndexPage;
				// ((IndexData)upEntry.data).setData(newIndexPageId)
				((IndexData) shiftdataup.data).setData(newSplitIndex.getCurPage());
				// Returning upEntry
				return shiftdataup;
			}
		}
		return null;
	}

	public boolean Delete(KeyClass key, RID rid)
			throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
			KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException, IOException {
		// If the database is set to use naive deletion, the method calls the NaiveDelete method to perform the deletion.
		if (headerPage.get_deleteFashion() == DeleteFashion.NAIVE_DELETE)
			return NaiveDelete(key, rid);
		else
			// If the database is not set to use naive deletion, the method throws a DeleteFashionException, 
			// indicating that the desired deletion method is not supported.
			throw new DeleteFashionException(null, "");
	}

	BTLeafPage findRunStart(KeyClass lo_key, RID startrid) throws IOException, IteratorException, KeyNotMatchException,
			ConstructPageException, PinPageException, UnpinPageException {
		BTLeafPage pageLeaf;
		BTIndexPage pageIndex;
		Page page;
		BTSortedPage sortPage;
		PageId pgnum, prepgnum, nextpgno;
		PageId currpgnum = null; // Iterator
		RID curRid;
		KeyDataEntry currentry;

		pgnum = headerPage.get_rootId();

		// base case
		if (pgnum.pid == INVALID_PAGE) { // no pages in the BTREE
			pageLeaf = null; // should be handled by.
			return pageLeaf;
		}

		page = pinPage(pgnum);
		sortPage = new BTSortedPage(page, headerPage.get_keyType());

		if (trace != null) {
			trace.writeBytes("VISIT node " + pgnum + lineSep);
			trace.flush();
		}

		// ASSERTION - pageno and sortPage is the root of the btree
		// pageno and sortPage valid and pinned

		while (sortPage.getType() == NodeType.INDEX) {
			pageIndex = new BTIndexPage(page, headerPage.get_keyType());
			prepgnum = pageIndex.getPrevPage();
			currentry = pageIndex.getFirst(startrid);
			while (currentry != null && lo_key != null && BT.keyCompare(currentry.key, lo_key) < 0) {

				prepgnum = ((IndexData) currentry.data).getData();
				currentry = pageIndex.getNext(startrid);
			}

			unpinPage(pgnum);

			pgnum = prepgnum;
			page = pinPage(pgnum);
			sortPage = new BTSortedPage(page, headerPage.get_keyType());

			if (trace != null) {
				trace.writeBytes("VISIT node " + pgnum + lineSep);
				trace.flush();
			}

		}

		pageLeaf = new BTLeafPage(page, headerPage.get_keyType());

		currentry = pageLeaf.getFirst(startrid);
		while (currentry == null) {
			nextpgno = pageLeaf.getNextPage();
			unpinPage(pgnum);
			if (nextpgno.pid == INVALID_PAGE) {
				return null;
			}

			pgnum = nextpgno;
			pageLeaf = new BTLeafPage(pinPage(pgnum), headerPage.get_keyType());
			currentry = pageLeaf.getFirst(startrid);
		}
		// - curkey, curRid: contain the first record
		if (lo_key == null) {
			return pageLeaf;
			// note that pageno/pageLeaf is still pinned;
			// scan will unpin it when done
		}
		while (BT.keyCompare(currentry.key, lo_key) < 0) {
			currentry = pageLeaf.getNext(startrid);
			while (currentry == null) {
				nextpgno = pageLeaf.getNextPage();
				unpinPage(pgnum);

				if (nextpgno.pid == INVALID_PAGE) {
					return null;
				}
				pgnum = nextpgno;
				pageLeaf = new BTLeafPage(pinPage(pgnum), headerPage.get_keyType());
				currentry = pageLeaf.getFirst(startrid);
			}
		}
		return pageLeaf;
	}

	/**
	 * This is the method signature that specifies the method's access modifier, name, parameter types, and any exceptions that it throws
	 * Declare and initialize a BTLeafPage object called leafPage and a RID object called ridItr. Also declare a KeyDataEntry object called kDataEntry. 
	 * Assign the result of calling the findRunStart method with the key and ridItr as arguments to the leafPage variable.
	 */
	private boolean NaiveDelete(KeyClass key, RID rid)
			throws LeafDeleteException, KeyNotMatchException, PinPageException, ConstructPageException, IOException,
			UnpinPageException, PinPageException, IndexSearchException, IteratorException

	{
		BTLeafPage leafPage;
		RID ridItr = new RID();
		KeyDataEntry kDataEntry;
		leafPage = findRunStart(key, ridItr);
		if (leafPage == null)
			return false;
		//If the leafPage is null, return false because the key was not found.
		kDataEntry = leafPage.getCurrent(ridItr);
		RID fRID = new RID();
		int dlt = 0;
		//Enter a do-while loop that loops until a break statement is encountered
		do {
			while (kDataEntry == null) {
				//Inside the loop, enter a while loop that loops until kDataEntry is not null
				PageId nextPage = leafPage.getNextPage();
				unpinPage(leafPage.getCurPage());
				if (nextPage.pid == INVALID_PAGE)
					return false;
				leafPage = new BTLeafPage(nextPage, headerPage.get_keyType());
				//getFirst method of the leafPage object with fRID as an argument and assign the result to kDataEntry.
				kDataEntry = leafPage.getFirst(fRID);
			}
			// If the key is greater than the key in the kDataEntry, break out of the loop
			if (BT.keyCompare(key, kDataEntry.key) > 0)
				break;
			// Enter a while loop that continues until the delEntry method of the leafPage object with a new 
			// KeyDataEntry object with key and rid as arguments returns false. Inside the loop, update kDataEntry 
			// to the current entry at ridItr and update dlt
			while (leafPage.delEntry(new KeyDataEntry(key, rid)) == true) {
				kDataEntry = leafPage.getCurrent(ridItr);
				dlt = 1;
			}
			if (kDataEntry == null)
				continue;
			else
				break;
		} while (true);
		unpinPage(leafPage.getCurPage());
		if (dlt == 1)
			return true;
		else
			return false;
	}

	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key) throws IOException, KeyNotMatchException,
			IteratorException, ConstructPageException, PinPageException, UnpinPageException

	{
		BTFileScan scan = new BTFileScan();
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			scan.leafPage = null;
			return scan;
		}
		scan.treeFilename = dbname;
		scan.endkey = hi_key;
		scan.didfirst = false;
		scan.deletedcurrent = false;
		scan.curRid = new RID();
		scan.keyType = headerPage.get_keyType();
		scan.maxKeysize = headerPage.get_maxKeySize();
		scan.bfile = this;
		scan.leafPage = findRunStart(lo_key, scan.curRid);
		return scan;
	}

	void trace_children(PageId id)
			throws IOException, IteratorException, ConstructPageException, PinPageException, UnpinPageException {

		if (trace != null) {
			BTSortedPage sortedPage;
			RID metaRid = new RID();
			PageId childPageId;
			KeyClass key;
			KeyDataEntry entry;
			sortedPage = new BTSortedPage(pinPage(id), headerPage.get_keyType());
			if (sortedPage.getType() == NodeType.INDEX) {
				BTIndexPage indexPage = new BTIndexPage(sortedPage, headerPage.get_keyType());
				trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
				trace.writeBytes(" " + indexPage.getPrevPage());
				for (entry = indexPage.getFirst(metaRid); entry != null; entry = indexPage.getNext(metaRid)) {
					trace.writeBytes("   " + ((IndexData) entry.data).getData());
				}
			} else if (sortedPage.getType() == NodeType.LEAF) {
				BTLeafPage leafPage = new BTLeafPage(sortedPage, headerPage.get_keyType());
				trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
				for (entry = leafPage.getFirst(metaRid); entry != null; entry = leafPage.getNext(metaRid)) {
					trace.writeBytes("   " + entry.key + " " + entry.data);
				}
			}
			unpinPage(id);
			trace.writeBytes(lineSep);
			trace.flush();
		}

	}
}
