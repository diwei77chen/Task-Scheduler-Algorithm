import java.util.ArrayList;
import java.util.Scanner;
import java.util.Comparator;
import java.util.Arrays;
import java.util.Collection;
import java.io.*;


// Author: Diwei Chen
// Date Revised: 20 Jun 2015

public class TaskScheduler{
	/**Time complexity analysis of the scheduler method:
	 * 
	 * This method makes use of the data structure priority heap to schedule the given tasks 
	 * by their release time and deadline. 
	 * As the time complexity of the insert method(add) of PriorityQueue, related to upHeap(siftUp) is O(log n), 
	 * n is the number of given tasks, for n tasks inserted to a heap, the time complexity is O(n log n). 
	 * As the time complexity of the removed method(poll) of PriorityQueue, related to downHeap(siftDown) is O(log n),
	 * n is the number of given tasks, for n tasks removed from a heap, the time complexity is O(n log n).
	 *
	 * In this method, firstly, n tasks is inserted to heap1, that is O(n log n). At each time, at most removes
	 * m tasks from heap1 and inserts them to heap2, m is a constant and that at most can be done 2*n times, so that
	 * is O(n log n).
	 * Removes n tasks from heap2 and adds them to an array list, that is O(n log n). 
	 * Therefore, the time complexity is O(n log n) + O(n log n) + O(n log n), e.g, O(n log n).
	 */
	
	/** This class methods gets a task set from file1, constructs a feasible schedule for
	 *  the task set on a processor with m identical cores by using the EDF strategy, 
	 *  and write the feasible schedule to file2. If no feasible schedule exists, 
	 *  it displays “No feasible schedule exists” on the screen.
	 */
	static void scheduler(String file1, String file2, Integer m){
		//used to read the strings from file1
		String fileStrArr1[] = null;		
		
		//used to write the output tasks to file2
		ArrayList<Task> writeArrList = new ArrayList<Task>();	
		
		//heap1 stores all the tasks from the given file and sorts them by their release time
		PriorityQueue<Task> heap1 = new PriorityQueue<Task>(new Comparator<Task>(){
					public int compare(Task t1, Task t2){
						return t1.getRelTime() - t2.getRelTime();
					}
		});
		
		//heap2 sorts the tasks by their deadline and transfers the tasks which can be executed in 
		//the time unit to writeArrList
		PriorityQueue<Task> heap2 = new PriorityQueue<Task>(new Comparator<Task>(){
					public int compare(Task t1, Task t2){
						return t1.getDeadline() - t2.getDeadline();
					}
		});
		
		//reads strings of the given file and write them to the string list fileStrArr1[]
		fileStrArr1 = readFile(fileStrArr1, file1);
		
		//If the file is improperly formatted, system exists.
		isFormat(fileStrArr1, file1);
		
		//adds the tasks from file string array1 to heap1 and sorts them by their release time
		heap1 = addStrArrToHeap(fileStrArr1, heap1);
		
		String path = file2 + ".txt";
		File file = new File(path);
		
		//counts the executed times 
		int timeCounter = 0;		
		
		int fileFlag = 1;
		
		while(heap1.size() > 1){
			
			//heap1Head always points to the head of heap1
			Task heap1Head = heap1.peek();
			
			//if the executed time is between the release time and the deadline of the latest task
			if(heap1.peek().getRelTime() <= timeCounter && timeCounter < heap1.peek().getDeadline()){
				heap1Head = heap1.poll();
				heap2.add(heap1Head);
				heap1Head = heap1.peek();
			
				//at most removes m tasks from heap1 and adds them to heap2
				for(int t = 1; t < m; ++t){
					//if the release time of heap1Head is equals to that of heap2Head,
					//consistently puts the head of heap1 into heap2
					if(heap1.peek().getRelTime() <= timeCounter && timeCounter < heap1.peek().getDeadline()){
						heap1Head = heap1.poll();
						heap2.add(heap1Head);
						if(heap1.size() == 0)
							break;
						else{
							heap1Head = heap1.peek();
						}
					}
				}
				
				Task writeTask = null;
				
				//executes the tasks in heap2, e.g., puts them to the output array list(writeArrList)
				while(heap2.size() > 0){
					writeTask = heap2.poll();
					writeArrList.add(writeTask);
				}
				++timeCounter;
				
			}
			else if(heap1.peek().getRelTime() > timeCounter){
				++timeCounter;
			}
			//if the executing time of the rest task(s) exceeds its deadline, then breaks and finishes
			else if(heap1.peek().getDeadline() <= timeCounter){
				System.out.printf("No feasible schedule exists on %d cores of %s.\n", m, file1);
				fileFlag = 0;
				break;
			}
		}
		//if there is only one task in heap1, executes it directly
		if(heap1.size() == 1 && heap1.peek().getRelTime() <= timeCounter
					&& timeCounter < heap1.peek().getDeadline()){		
			writeArrList.add(heap1.poll());
		}
		
		if(fileFlag == 1){
			//if file2 already exists, should remove file2 first and rerun the program
			if(file.exists()){
				System.out.printf("File %s already exists.\n", file2);
				System.out.printf("Please removes file %s and reruns routine.\n", file2);
				fileFlag = 0;
			}
			else if(fileFlag == 1){
				System.out.printf("There is a feasible schedule on %d cores of %s.\n", m, file1);
				writeFile(writeArrList, file.toString());
		
			}
		}
	}
	
	/**	This class method reads the strings of the given file and put them 
	 *  into a string array and returns the address of that string array.
	 */
	public static String[] readFile(String fileStrArr[], String f){
		Scanner input = null;
		String fileStr = "";
		String path = f;
		File file = new File(path);
		try{
			input = new Scanner(file);		//puts the file into scan stream
		}
		catch(FileNotFoundException fileNotFoundException){
			System.err.printf("%s does not exist.\n", f);
			System.exit(1);
		}
		try{
			while(input.hasNextLine()){
					fileStr = fileStr + input.nextLine() + " ";
			}
			fileStrArr = fileStr.split("\\s+");		//splits the string by whitespace
		}
		catch(IllegalStateException stateException){
			System.out.printf("Erro reading from %s.\n", f);
		}
		input.close();
		return fileStrArr;
	}

	/** This class method returns whether the file is formatted properly.
	 */
	public static boolean isFormat(String fileStrArr[], String f){
		for(int i = 0; i < fileStrArr.length; ++i){
			if(i % 3 == 0 && fileStrArr[i].matches("[a-z]{1,100}[0-9]*"))		//a task name is a string of letters and numbers
				continue;
			else if(i % 3 == 1 && fileStrArr[i].matches("[0-9]*"))		//all the release times are non-negative integers
				continue;
			else if(i % 3 == 2 && fileStrArr[i].matches("[0-9]*"))		//all the deadlines are natural numbers
				continue;
			else{
				System.out.printf("%s improperly formatted.\n", f);
				System.exit(1);
			}
		}
		return true;
	}

	/**This class method converts the given file string to Task nodes and adds these nodes
	 * to a task array list then returns this array list
	 */
	public static PriorityQueue<Task> addStrArrToHeap(String f[], PriorityQueue<Task> heap){

		for(int i = 0; i < f.length; i = i + 3){
			Task task = new Task();
			task.setName(f[i]);			//sets the task name from the string array f sequentially
			task.setRelTime(Integer.parseInt(f[i+1]));		//sets the release time from the string array f sequentially
			task.setDeadline(Integer.parseInt(f[i+2]));		//sets the deadline from the string array f sequentially
			heap.add(task);	
		}
		return heap;
	}
	
	/** This class method creates a text file with a given name and writes the task name, release time 
	 * and deadline of each given task into it.
	 */
	public static void writeFile(ArrayList<Task> arr, String f){
		try{
			PrintWriter writer = new PrintWriter(f, "UTF-8");
			
			for(int m = 0; m < arr.size(); ++m){
				writer.print(arr.get(m).getName() + " " +
								arr.get(m).getRelTime() + " " + 
									arr.get(m).getDeadline() + "\n");
			}
			writer.close();
		}
		catch(IOException ex){
			
		}
		
	}
}


/** Class representing a node of a priority queue by sorting 
 *  references to the release time or deadline
 */
class Task {
	private String name;		//the name of the task
	private int relTime;		//the release time of the task
	private int deadline;		//the deadline of the task
	
	public Task(){}
	
	public Task(String n, int r, int d){
		name = n;
		relTime = r;
		deadline = d;
	}
	
	/* sets the task name*/
	protected void setName(String n){
		name = n;
	}
	/* sets the release time*/
	protected void setRelTime(int r){
		relTime = r;
	}
	/* sets the deadline*/
	protected void setDeadline(int d){
		deadline = d;
	}
	
	/* gets the task name*/
	protected String getName(){
		return name;
	}
	/* gets the release time*/
	protected int getRelTime(){
		return relTime;
	}
	/* gets the deadline*/
	protected int getDeadline(){
		return deadline;
	}
}

/** This is the source code from java.util.PriorityQueue,
 *  but only a part of them is copied and used in TaskScheduler.
 */
class PriorityQueue<E> {
    private static final int DEFAULT_INITIAL_CAPACITY = 11;

    /**
     * Priority queue represented as a balanced binary heap: the two
     * children of queue[n] are queue[2*n+1] and queue[2*(n+1)].  The
     * priority queue is ordered by comparator, or by the elements'
     * natural ordering, if comparator is null: For each node n in the
     * heap and each descendant d of n, n <= d.  The element with the
     * lowest value is in queue[0], assuming the queue is nonempty.
     */
    transient Object[] queue; // non-private to simplify nested class access

    /**
     * The number of elements in the priority queue.
     */
    private int size = 0;

    /**
     * The comparator, or null if priority queue uses elements'
     * natural ordering.
     */
    private final Comparator<? super E> comparator;

    /**
     * The number of times this priority queue has been
     * <i>structurally modified</i>.  See AbstractList for gory details.
     */
    transient int modCount = 0; // non-private to simplify nested class access

    /**
     * Creates a {@code PriorityQueue} with the default initial
     * capacity (11) that orders its elements according to their
     * {@linkplain Comparable natural ordering}.
     */
    public PriorityQueue() {
        this(DEFAULT_INITIAL_CAPACITY, null);
    }
    
    /**
     * Creates a {@code PriorityQueue} with the default initial capacity and
     * whose elements are ordered according to the specified comparator.
     *
     * @param  comparator the comparator that will be used to order this
     *         priority queue.  If {@code null}, the {@linkplain Comparable
     *         natural ordering} of the elements will be used.
     * @since 1.8
     */
    public PriorityQueue(Comparator<? super E> comparator) {
        this(DEFAULT_INITIAL_CAPACITY, comparator);
    }

    
    /**
     * Creates a {@code PriorityQueue} with the specified initial capacity
     * that orders its elements according to the specified comparator.
     *
     * @param  initialCapacity the initial capacity for this priority queue
     * @param  comparator the comparator that will be used to order this
     *         priority queue.  If {@code null}, the {@linkplain Comparable
     *         natural ordering} of the elements will be used.
     * @throws IllegalArgumentException if {@code initialCapacity} is
     *         less than 1
     */
    public PriorityQueue(int initialCapacity,
                         Comparator<? super E> comparator) {
        // Note: This restriction of at least one is not actually needed,
        // but continues for 1.5 compatibility
        if (initialCapacity < 1)
            throw new IllegalArgumentException();
        this.queue = new Object[initialCapacity];
        this.comparator = comparator;
    }
    /**
     * The maximum size of array to allocate.
     * Some VMs reserve some header words in an array.
     * Attempts to allocate larger arrays may result in
     * OutOfMemoryError: Requested array size exceeds VM limit
     */
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
    /**
     * Increases the capacity of the array.
     *
     * @param minCapacity the desired minimum capacity
     */
    private void grow(int minCapacity) {
        int oldCapacity = queue.length;
        // Double size if small; else grow by 50%
        int newCapacity = oldCapacity + ((oldCapacity < 64) ?
                                         (oldCapacity + 2) :
                                         (oldCapacity >> 1));
        // overflow-conscious code
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);
        queue = Arrays.copyOf(queue, newCapacity);
    }
    
    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError();
        return (minCapacity > MAX_ARRAY_SIZE) ?
            Integer.MAX_VALUE :
            MAX_ARRAY_SIZE;
    }
    
    /**
     * Inserts the specified element into this priority queue.
     *
     * @return {@code true} (as specified by {@link Collection#add})
     * @throws ClassCastException if the specified element cannot be
     *         compared with elements currently in this priority queue
     *         according to the priority queue's ordering
     * @throws NullPointerException if the specified element is null
     */
    public boolean add(E e) {
        return offer(e);
    }
    
    /**
     * Inserts the specified element into this priority queue.
     *
     * @return {@code true} (as specified by {@link Queue#offer})
     * @throws ClassCastException if the specified element cannot be
     *         compared with elements currently in this priority queue
     *         according to the priority queue's ordering
     * @throws NullPointerException if the specified element is null
     */
    public boolean offer(E e) {
        if (e == null)
            throw new NullPointerException();
        modCount++;
        int i = size;
        if (i >= queue.length)
            grow(i + 1);
        size = i + 1;
        if (i == 0)
            queue[0] = e;
        else
            siftUp(i, e);
        return true;
    }
    @SuppressWarnings("unchecked")
    public E peek() {
        return (size == 0) ? null : (E) queue[0];
    }

    private int indexOf(Object o) {
        if (o != null) {
            for (int i = 0; i < size; i++)
                if (o.equals(queue[i]))
                    return i;
        }
        return -1;
    }

    /**
     * Removes a single instance of the specified element from this queue,
     * if it is present.  More formally, removes an element {@code e} such
     * that {@code o.equals(e)}, if this queue contains one or more such
     * elements.  Returns {@code true} if and only if this queue contained
     * the specified element (or equivalently, if this queue changed as a
     * result of the call).
     *
     * @param o element to be removed from this queue, if present
     * @return {@code true} if this queue changed as a result of the call
     */
    public boolean remove(Object o) {
        int i = indexOf(o);
        if (i == -1)
            return false;
        else {
            removeAt(i);
            return true;
        }
    }

    /**
     * Version of remove using reference equality, not equals.
     * Needed by iterator.remove.
     *
     * @param o element to be removed from this queue, if present
     * @return {@code true} if removed
     */
    boolean removeEq(Object o) {
        for (int i = 0; i < size; i++) {
            if (o == queue[i]) {
                removeAt(i);
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if this queue contains the specified element.
     * More formally, returns {@code true} if and only if this queue contains
     * at least one element {@code e} such that {@code o.equals(e)}.
     *
     * @param o object to be checked for containment in this queue
     * @return {@code true} if this queue contains the specified element
     */
    public boolean contains(Object o) {
        return indexOf(o) != -1;
    }

    /**
     * Returns an array containing all of the elements in this queue.
     * The elements are in no particular order.
     *
     * <p>The returned array will be "safe" in that no references to it are
     * maintained by this queue.  (In other words, this method must allocate
     * a new array).  The caller is thus free to modify the returned array.
     *
     * <p>This method acts as bridge between array-based and collection-based
     * APIs.
     *
     * @return an array containing all of the elements in this queue
     */
    public Object[] toArray() {
        return Arrays.copyOf(queue, size);
    }

    /**
     * Returns an array containing all of the elements in this queue; the
     * runtime type of the returned array is that of the specified array.
     * The returned array elements are in no particular order.
     * If the queue fits in the specified array, it is returned therein.
     * Otherwise, a new array is allocated with the runtime type of the
     * specified array and the size of this queue.
     *
     * <p>If the queue fits in the specified array with room to spare
     * (i.e., the array has more elements than the queue), the element in
     * the array immediately following the end of the collection is set to
     * {@code null}.
     *
     * <p>Like the {@link #toArray()} method, this method acts as bridge between
     * array-based and collection-based APIs.  Further, this method allows
     * precise control over the runtime type of the output array, and may,
     * under certain circumstances, be used to save allocation costs.
     *
     * <p>Suppose {@code x} is a queue known to contain only strings.
     * The following code can be used to dump the queue into a newly
     * allocated array of {@code String}:
     *
     *  <pre> {@code String[] y = x.toArray(new String[0]);}</pre>
     *
     * Note that {@code toArray(new Object[0])} is identical in function to
     * {@code toArray()}.
     *
     * @param a the array into which the elements of the queue are to
     *          be stored, if it is big enough; otherwise, a new array of the
     *          same runtime type is allocated for this purpose.
     * @return an array containing all of the elements in this queue
     * @throws ArrayStoreException if the runtime type of the specified array
     *         is not a supertype of the runtime type of every element in
     *         this queue
     * @throws NullPointerException if the specified array is null
     */
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        final int size = this.size;
        if (a.length < size)
            // Make a new array of a's runtime type, but my contents:
            return (T[]) Arrays.copyOf(queue, size, a.getClass());
        System.arraycopy(queue, 0, a, 0, size);
        if (a.length > size)
            a[size] = null;
        return a;
    }

    public int size() {
        return size;
    }

    /**
     * Removes all of the elements from this priority queue.
     * The queue will be empty after this call returns.
     */
    public void clear() {
        modCount++;
        for (int i = 0; i < size; i++)
            queue[i] = null;
        size = 0;
    }

    @SuppressWarnings("unchecked")
    public E poll() {
        if (size == 0)
            return null;
        int s = --size;
        modCount++;
        E result = (E) queue[0];
        E x = (E) queue[s];
        queue[s] = null;
        if (s != 0)
            siftDown(0, x);
        return result;
    }

    /**
     * Removes the ith element from queue.
     *
     * Normally this method leaves the elements at up to i-1,
     * inclusive, untouched.  Under these circumstances, it returns
     * null.  Occasionally, in order to maintain the heap invariant,
     * it must swap a later element of the list with one earlier than
     * i.  Under these circumstances, this method returns the element
     * that was previously at the end of the list and is now at some
     * position before i. This fact is used by iterator.remove so as to
     * avoid missing traversing elements.
     */
    @SuppressWarnings("unchecked")
    private E removeAt(int i) {
        // assert i >= 0 && i < size;
        modCount++;
        int s = --size;
        if (s == i) // removed last element
            queue[i] = null;
        else {
            E moved = (E) queue[s];
            queue[s] = null;
            siftDown(i, moved);
            if (queue[i] == moved) {
                siftUp(i, moved);
                if (queue[i] != moved)
                    return moved;
            }
        }
        return null;
    }

    /**
     * Inserts item x at position k, maintaining heap invariant by
     * promoting x up the tree until it is greater than or equal to
     * its parent, or is the root.
     *
     * To simplify and speed up coercions and comparisons. the
     * Comparable and Comparator versions are separated into different
     * methods that are otherwise identical. (Similarly for siftDown.)
     *
     * @param k the position to fill
     * @param x the item to insert
     */
    private void siftUp(int k, E x) {
        if (comparator != null)
            siftUpUsingComparator(k, x);
        else
            siftUpComparable(k, x);
    }

    @SuppressWarnings("unchecked")
    private void siftUpComparable(int k, E x) {
        Comparable<? super E> key = (Comparable<? super E>) x;
        while (k > 0) {
            int parent = (k - 1) >>> 1;
            Object e = queue[parent];
            if (key.compareTo((E) e) >= 0)
                break;
            queue[k] = e;
            k = parent;
        }
        queue[k] = key;
    }

    @SuppressWarnings("unchecked")
    private void siftUpUsingComparator(int k, E x) {
        while (k > 0) {
            int parent = (k - 1) >>> 1;
            Object e = queue[parent];
            if (comparator.compare(x, (E) e) >= 0)
                break;
            queue[k] = e;
            k = parent;
        }
        queue[k] = x;
    }

    /**
     * Inserts item x at position k, maintaining heap invariant by
     * demoting x down the tree repeatedly until it is less than or
     * equal to its children or is a leaf.
     *
     * @param k the position to fill
     * @param x the item to insert
     */
    private void siftDown(int k, E x) {
        if (comparator != null)
            siftDownUsingComparator(k, x);
        else
            siftDownComparable(k, x);
    }

    @SuppressWarnings("unchecked")
    private void siftDownComparable(int k, E x) {
        Comparable<? super E> key = (Comparable<? super E>)x;
        int half = size >>> 1;        // loop while a non-leaf
        while (k < half) {
            int child = (k << 1) + 1; // assume left child is least
            Object c = queue[child];
            int right = child + 1;
            if (right < size &&
                ((Comparable<? super E>) c).compareTo((E) queue[right]) > 0)
                c = queue[child = right];
            if (key.compareTo((E) c) <= 0)
                break;
            queue[k] = c;
            k = child;
        }
        queue[k] = key;
    }

    @SuppressWarnings("unchecked")
    private void siftDownUsingComparator(int k, E x) {
        int half = size >>> 1;
        while (k < half) {
            int child = (k << 1) + 1;
            Object c = queue[child];
            int right = child + 1;
            if (right < size &&
                comparator.compare((E) c, (E) queue[right]) > 0)
                c = queue[child = right];
            if (comparator.compare(x, (E) c) <= 0)
                break;
            queue[k] = c;
            k = child;
        }
        queue[k] = x;
    }

    /**
     * Establishes the heap invariant (described above) in the entire tree,
     * assuming nothing about the order of the elements prior to the call.
     */
    @SuppressWarnings("unchecked")
    private void heapify() {
        for (int i = (size >>> 1) - 1; i >= 0; i--)
            siftDown(i, (E) queue[i]);
    }

    /**
     * Returns the comparator used to order the elements in this
     * queue, or {@code null} if this queue is sorted according to
     * the {@linkplain Comparable natural ordering} of its elements.
     *
     * @return the comparator used to order this queue, or
     *         {@code null} if this queue is sorted according to the
     *         natural ordering of its elements
     */
    public Comparator<? super E> comparator() {
        return comparator;
    }
}