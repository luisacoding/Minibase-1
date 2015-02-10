package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;
import bufmgr.FrameDesc;

import java.util.HashMap;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager manages an array of main memory pages.  The array is
 * called the buffer pool, each page is called a frame.  
 * It provides the following services:
 * <ol>
 * <li>Pinning and unpinning disk pages to/from frames
 * <li>Allocating and deallocating runs of disk pages and coordinating this with
 * the buffer pool
 * <li>Flushing pages from the buffer pool
 * <li>Getting relevant data
 * </ol>
 * The buffer manager is used by access methods, heap files, and
 * relational operators.
 */
public class BufMgr implements GlobalConst {
	
	private int numFrames;
	
	// The last page that was replaced.
	private int lastVictim = 0 ;
	
	// Array of type Page and array name is bufferpool.
	// This array holds Page objects.
	private Page[] bufferPool;
	
	//Array of type FrameDesc and array name is frametab
	//This array holds FrameDesc type objects
	private FrameDesc[] frametab;
	
	//framePageMapper - Object of type hash map 
	private HashMap<Integer, Integer> framePageMapper ;


  /**
   * Constructs a buffer manager by initializing member data.  
   * 
   * @param numframes number of frames in the buffer pool
   */
  public BufMgr(int numframes) {
	  
	  // Initializing the size of the bufferlPool array to the number of numframes.
	  bufferPool = new Page[numframes];
	   numFrames = numframes ;
	  
	  // Intializing the size of the frametab array to the number of numframes.
	  frametab = new FrameDesc[numframes]; 
	  
	  framePageMapper = new HashMap<Integer,Integer>(numframes) ;
	  
	
	      for( int i = 0; i < frametab.length ; i++ ) {
		  frametab[i] = new FrameDesc() ;
		  framePageMapper.remove(i);
		  bufferPool[i] = new Page();
	  }
	
    //throw new UnsupportedOperationException("Not implemented");

  } // public BufMgr(int numframes)

  /**
   * The result of this call is that disk page number pageno should reside in
   * a frame in the buffer pool and have an additional pin assigned to it, 
   * and mempage should refer to the contents of that frame. <br><br>
   * 
   * If disk page pageno is already in the buffer pool, this simply increments
   * the pin count.  Otherwise, this<br> 
   * <pre>
   * 	uses the replacement policy to select a frame to replace
   * 	writes the frame's contents to disk if valid and dirty
   * 	if (contents == PIN_DISKIO)
   * 		read disk page pageno into chosen frame
   * 	else (contents == PIN_MEMCPY)
   * 		copy mempage into chosen frame
   * 	[omitted from the above is maintenance of the frame table and hash map]
   * </pre>		
   * @param pageno identifies the page to pin
   * @param mempage An output parameter referring to the chosen frame.  If
   * contents==PIN_MEMCPY it is also an input parameter which is copied into
   * the chosen frame, see the contents parameter. 
   * @param contents Describes how the contents of the frame are determined.<br>  
   * If PIN_DISKIO, read the page from disk into the frame.<br>  
   * If PIN_MEMCPY, copy mempage into the frame.<br>  
   * If PIN_NOOP, copy nothing into the frame - the frame contents are irrelevant.<br>
   * Note: In the cases of PIN_MEMCPY and PIN_NOOP, disk I/O is avoided.
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned.
   * @throws IllegalStateException if all pages are pinned (i.e. pool is full)
   */
  public void pinPage(PageId pageno, Page mempage, int contents) {
	  
	  //Print what page number we are on currently.
	  System.out.println("pinpage on " + pageno.pid);
      int index = -1; // QUESTION ??I dont know why are we setting a new variable index to -1
      
      //We are searching for the page in the Index. When page is not in the pool.
      if (!framePageMapper.containsKey(pageno.pid)) {
          System.out.println("Page " + pageno.pid + " not in pool...");
        
          /**If a new page has to be brought in, we need a page to move out of the bufferPool to bring in a new one
           * Using the Clock Replacement method, lastVictim() method - to look for replacement
           */
          
          if (index == -1) {
              System.out.println("Finding replacement...");
              
          // Initializing a new variable toReplace to contain the index of the frame that is chosen for replacement.
              int toReplace;
              try {
            	  
            	  //pickVictim gets the index of the frame to replace.
                  toReplace = pickVictim(); 
              } catch (IllegalStateException e) {
                  throw e;
              }
              
              //QUESTION?? y are we doing this here, isnt the lastVictim method taking care of this ??
              // If the page to replace is dirty perform write on disk
              if (frametab[toReplace].dirty == true) {
                  Minibase.DiskManager.write_page(frametab[toReplace].pageId, bufferPool[toReplace]);
              }
              
              
              //Clean frame to pin new page
              // Remove the Entry of the frame selected from the hashmap Index
              framePageMapper.remove(frametab[toReplace].pageId.pid);
              
              //set the value of the valid to false.
              frametab[toReplace].valid = false;
              
              //set the dirty bit to false, as there is no data that needs to be synced between  disk and page
              frametab[toReplace].dirty = false;
              
              //the id of the page is set to negative
              frametab[toReplace].pageId.pid = -1;
              
              //The pincount of the page has been set to 0
              frametab[toReplace].pinCount= 0;
              index = toReplace;

          }
          
          /**
           * There are three things you can do when you bring in a page
           * 1. Just get the page in. create space in bufferpool for bringing the page in. - DISKIO
           * 2. Getting the page ready for use by the requester. Copying the contents from the memory to use, and 
           * pinning the page. - MEMCPY
           */
        //Put the new requested page into the bufferPool.
          if (contents == PIN_DISKIO) {
              System.out.println("Pinning diskio...");
              
              /**
               * * Reads the contents of the specified page from disk.
               * @param pageno identifies the page to read.  It is the page number
               * in the OS file.  Also referred to as the Id of the page.
               * @param mempage output parameter to hold the contents of the page
               */
              Minibase.DiskManager.read_page(pageno, bufferPool[index]);
            
              //copying the Id of that page and assign it to the new page that we are bringing in the pool.
              frametab[index].pageId.copyPageId(pageno);
              
              // As this is a recently brought in page it has nothing written onto it, so dirty bit is false
              frametab[index].dirty = false;
              
              // This is a Valid page that we are getting in
              frametab[index].valid = true;
              
              //incrementing the pincount of the page by 1.
              frametab[index].pinCount++;
              
              //Adding the value of key value pair for hash map.
              framePageMapper.put(pageno.pid, index);
              
              //
              mempage.setData(bufferPool[index].getData());
              
            
          } else if (contents == PIN_MEMCPY) {
              System.out.println("Pinning memcopy...");
              
              //bufferpool[index] = new Page();
              bufferPool[index].setPage(mempage);
              
              //
              frametab[index].pageId.copyPageId(pageno);
           
              frametab[index].dirty = false;
              frametab[index].valid = true;
              frametab[index].pinCount++;
              framePageMapper.put(pageno.pid, index);
         
              
          } else if (contents == PIN_NOOP) {
        	  frametab[index].pageId.copyPageId(pageno);
              //frametab[index].pageId = pageno;
              frametab[index].dirty = false;
              frametab[index].valid = true;
              frametab[index].pinCount++;
              framePageMapper.put(pageno.pid, index);
              
              // Set the page in bufferpool
              mempage.setPage(bufferPool[index]);
              mempage.setData(bufferPool[index].getData());
              System.out.println("Noop");
              
              
            //NOOP
          }
      } else {
          System.out.println("Page already in buffer...");
          //Page exists in buffer pool
          
         
          index = framePageMapper.get(pageno.pid);
          //Page is already pinned
          
          
          // Error Handling
          if ((contents == PIN_MEMCPY) && (frametab[index].pinCount != 0)) {
              throw new IllegalArgumentException("Page already pinned; Pin aborted");
          }
          
          frametab[index].pinCount++;
          mempage.setPage(bufferPool[index]);
          mempage.setData(bufferPool[index].getData());
      }
      //throw new UnsupportedOperationException("Not implemented");

  } // public void pinPage(PageId pageno, Page page, int contents)

  
  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
   * @throws IllegalArgumentException if the page is not in the buffer pool
   *  or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) {
	  
	  // Check 1. if pageno does not exist in framePageMapper
	  // Check 2. if the PinCount of the pageno is 0.
	  if (!framePageMapper.containsKey(pageno.pid) || frametab[framePageMapper.get(pageno.pid)].getPinCount() == 0)
	  {
		  throw new IllegalArgumentException(" Page is not in buffer pool or this Page is not pinned") ;
	  }
	  else
	  {
		  // declare a new variable pincount which stores the decreased value of pincount
		  // if this is the page to be unpinned.
		  int pincount = frametab[framePageMapper.get(pageno.pid)].getPinCount() - 1 ;
		  
		  // set the pincount to new value
		  frametab[framePageMapper.get(pageno.pid)].setPinCount(pincount);
		  
		  //set refBit to true if pinCount is 0
		  // i.e., the page was recently accessed and hence the reference bit is set to True.
		  if(pincount == 0)
			  frametab[framePageMapper.get(pageno.pid)].setRefBit(true);
		  
		  // Check the boolean dirty 
		  if(dirty) 
			  frametab[framePageMapper.get(pageno.pid)].setDirty(UNPIN_DIRTY);
		  else
			  frametab[framePageMapper.get(pageno.pid)].setDirty(UNPIN_CLEAN);
	  }
	  
    //throw new UnsupportedOperationException("Not implemented");

  } // public void unpinPage(PageId pageno, boolean dirty)
  
  /**
   * Allocates a run of new disk pages and pins the first one in the buffer pool.
   * The pin will be made using PIN_MEMCPY.  Watch out for disk page leaks.
   * 
   * @param firstpg input and output: holds the contents of the first allocated page
   * and refers to the frame where it resides
   * @param run_size input: number of pages to allocate
   * @return page id of the first allocated page
   * @throws IllegalArgumentException if firstpg is already pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public PageId newPage(Page firstpg, int run_size) {
	  
      PageId pageno = new PageId();
	     try {
	            pageno.pid = Minibase.DiskManager.allocate_page(run_size).pid;
	            this.pinPage(pageno, firstpg, PIN_DISKIO);
	            //Minibase.DiskManager.read_page(pageno, firstpg);
	            //this.pinPage(pageno, firstpg, PIN_MEMCPY);
	        } catch (IllegalArgumentException | IllegalStateException exc) {
	            throw exc;
	        }
	        return pageno;
	        //throw new UnsupportedOperationException("Not implemented");

	    } // public PageId newPage(Page firstpg, int run_size)

 
  /**
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) {
	  
	  if (framePageMapper.containsKey(pageno.pid)) {
		  int index = framePageMapper.get(pageno.pid);
		  if (frametab[index].pinCount > 0) {
			  throw new IllegalArgumentException("Page is currently pinned");
		  }
		  frametab[index].valid = false;
		  frametab[index].dirty = false;
		  frametab[index].pageId.pid = -1;
		  framePageMapper.remove(pageno.pid);
		  //throw new IllegalArgumentException("Page not in buffer pool");
	  }
	  
      //bufferpool[index] = null;
      
      try {
          Minibase.DiskManager.deallocate_page(pageno);
      } catch (IllegalArgumentException exc) {
          throw exc;
      }
      //throw new UnsupportedOperationException("Not implemented");
  } 

  /**
   * Write all valid and dirty frames to disk.
   * Note flushing involves only writing, not unpinning or freeing
   * or the like.
   */
  public void flushAllFrames() {
	  
	  for(int index = 0 ; index < numFrames ; index++) {
		  
		  // Check if the page is valid and is dirty only then write it to the disc
		  if(frametab[index].getPageId().pid!= INVALID_PAGEID && frametab[index].isDirty())
		  {
			  
			  // Variable page contains the page that is Valid and Dirty and is to be written on disk
			  Page page = (bufferPool[index]);
			  
			  // Call write_page method and pass the page object to be written to the disc
			  Minibase.DiskManager.write_page(frametab[index].getPageId(),page);
		  }
	  }
	  
    // throw new UnsupportedOperationException("Not implemented");

  } // public void flushAllFrames()

  /**
   * Write a page in the buffer pool to disk, if dirty.
   * 
   * @throws IllegalArgumentException if the page is not in the buffer pool
   */
  public void flushPage(PageId pageno) {
	 
	  int index = framePageMapper.get(pageno);
	  Page page = (bufferPool[index]);
	  
	  // Write the page object onto the disc
	  Minibase.DiskManager.write_page(pageno, page);
	  
	//throw new UnsupportedOperationException("Not implemented");
    
  }

   /**
   * Gets the total number of buffer frames.
   */
  public int getNumFrames() {
	  return numFrames ;
	  
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  	
  public int getNumUnpinned() {
	  int unpinned = 0;
	  for(int index = 0; index < numFrames; index++) {
		  if(frametab[index].getPinCount() == 0 ) {
			  unpinned++ ;
		  }
	  }
	  return unpinned ;
  }  
  
  
	   /** This is the method created for the implementation of the Clock Replacement Policy 
	    * In order to replace a page in the frame:
	    * 1. The Page is NOT VALID
	    * 2. The page is PINNED i.e the pinCount > 0
	    * 3. Check the refBit, If true, we set it to false and look for next replacement page
	    * 3.1 If refBit is false, we have found our replacement
	    * 3.2 This page is then checked for isDirty, If yes, page is written on disk, else flushed.
	    * This pattern continues till we find the page that can be replaced
	    * However if we reach the same  place we started from and cannot find the page, another sweep is done
	    * If we still do not find the page an error message is sent
	    * @return
	    */
 
	  public int pickVictim() 
	  {
		  // Variable i is set as the frame which is next to the frame(lastVictim) that was last replaced.
	        int i = lastVictim + 1;
	        
	        //Check if we have already gone reached the end of the bufferPool array.
	        if(i == bufferPool.length) {
	        	i = 0;
	        }
	        
	        // As this is the first loop to go around the array containing pages 
	        // we set the looped variable boolean to false
	        boolean looped = false;
	        
	        // Check if the frame selected is VALID, if yes we found the replacement and lastVictim is set.
	        while (!looped) {
	        	if (frametab[i].valid != true){
	        		lastVictim = i ;
	        		return i ;
	        	}
	        	
	        	// Check if frame is VALID, and pincount is 0, and refBit is false, we found the replacement.
	            if (frametab[i].valid && frametab[i].pinCount == 0 && !frametab[i].refbit) {
	            	lastVictim = i ;
	                return i;
	            } 
	            
	            // Else the refBit is true and we set it to false.
	            else if (frametab[i].valid && frametab[i].pinCount == 0) {
	                frametab[i].refbit = false;
	            }
	            
	            //increment and check the next frame. Loop till the length of bufferPool
	            ++i;
	            if (i == bufferPool.length) {
	                i = 0;
	            }
	          
	            if (i == lastVictim + 1) {
	                looped = true;
	            }
	        }
	        looped = false;
	        
	        // We go around the frames the second time. Similar approach as loop 1.
	        while (!looped) {
	            if (frametab[i].valid && frametab[i].pinCount == 0 && !frametab[i].refbit) {
	            	lastVictim = i ;
	                return i;
	            } else if (frametab[i].valid && frametab[i].pinCount == 0) {
	                frametab[i].refbit = false;
	            }
	            ++i;
	            if (i == bufferPool.length) {
	                i = 0;
	            }
	            if (i == lastVictim + 1) {
	                looped = true;
	            }
	        }
	        
	  
	        throw new IllegalStateException("Cannot find page to replace");
	    }
		  
    //throw new UnsupportedOperationException("Not implemented");
  }

 
