/*
 * University of Warsaw
 * Concurrent Programming Course 2020/2021
 * Java Assignment
 * 
 * Author: Konrad Iwanicki (iwanicki@mimuw.edu.pl)
 */
package cp1.solution;

import java.lang.Long;
import java.util.Collection;
import java.util.Stack;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Semaphore;

import cp1.base.TransactionManager;
import cp1.base.LocalTimeProvider;
import cp1.base.Resource;
import cp1.base.ResourceId;
import cp1.base.ResourceOperation;

import cp1.base.AnotherTransactionActiveException;
import cp1.base.NoActiveTransactionException;
import cp1.base.UnknownResourceIdException;
import cp1.base.ActiveTransactionAborted;
import cp1.base.ResourceOperationException;


/**
 * A factory for instantiating transaction managers.
 * 
 * @author Konrad Iwanicki (iwanicki@mimuw.edu.pl)
 * @author Kacper Sołtysiak (ks418388@students.mimuw.edu.pl)
 */
public final class TransactionManagerFactory {

	/**
	 * Instantiates your solution: returns a
	 * new transaction manager that takes over
	 * control over a given collection of resources,
	 * to which end it uses local time as
	 * offered by a given provider.
	 * @param resources The collection of resources.
	 * @param timeProvider A local time provider.
	 * @return A new transaction manager for
	 *     controlling the resources.
	 */

	private static class TM implements TransactionManager {
		/* synchronisation semaphores */
		private Semaphore mutex;

		private AtomicBoolean waking;

		/* maps storing the transactions data */
		private ConcurrentMap<Thread, AtomicBoolean> transactionStatus;
		private ConcurrentMap<Thread, AtomicBoolean> abortedStatus;
		/* maps storing the transactions' data */
		private ConcurrentMap<Thread, Stack<ResourceOperation>> threadOperations;
		private ConcurrentMap<Thread, Stack<ResourceId>> threadOperationsTargets;
		private ConcurrentMap<Thread, AtomicLong> threadTransactionTime;
		/* maps managing the want-resource, own-resource relations and
		   queueing threads for desired resources */
		private ConcurrentMap<Resource, Semaphore> resourceAwait;
		private ConcurrentMap<Resource, Thread> resourceOwnership;
		private ConcurrentMap<Thread, Resource> wantedResource;
		/* data provided in constructor */
		private Collection<Resource> resources;
		private LocalTimeProvider timeProvider;


		public TM(Collection<Resource> resources,
				  LocalTimeProvider timeProvider) {

			this.resources = resources;
			this.timeProvider = timeProvider;

			this.transactionStatus = new ConcurrentHashMap<>();
			this.abortedStatus = new ConcurrentHashMap<>();

			this.threadOperations = new ConcurrentHashMap<>();
			this.threadOperationsTargets = new ConcurrentHashMap<>();
			this.threadTransactionTime = new ConcurrentHashMap<>();

			this.resourceOwnership = new ConcurrentHashMap<>();
			this.wantedResource = new ConcurrentHashMap<>();

			this.mutex = new Semaphore(1, true);

			this.resourceAwait = new ConcurrentHashMap<>();

			this.waking = new AtomicBoolean();

			for(Resource r: resources) {

				this.resourceAwait.computeIfAbsent(r, (k) -> new Semaphore(1, true));
			}

		}

		private boolean checkActiveTransaction() {

			return transactionStatus.computeIfAbsent(Thread.currentThread(), 
													 (k) -> new AtomicBoolean()).get();
		}

		private boolean checkAbortStatus() {

			return abortedStatus.computeIfAbsent(Thread.currentThread(), 
											     (k) -> new AtomicBoolean()).get();
		}

		private void setActiveTransactionStatus() {

			transactionStatus.computeIfAbsent(Thread.currentThread(),
											  (k) -> new AtomicBoolean()).set(true);
		}

		private void setAbortedStatus(Thread targetThread) {

			abortedStatus.computeIfAbsent(targetThread,
										  (k) -> new AtomicBoolean()).set(true);
		}

		private void setTransactionTime() {

			threadTransactionTime.computeIfAbsent(Thread.currentThread(),
												  (k) -> new AtomicLong(timeProvider.getTime()));
		}

		private Thread getOwnerOf(Resource targetResource) {

			if(targetResource == null) {

				return null;
			}
			else {
				
				return resourceOwnership.get(targetResource);
			}
		}

		private long getTimeOfTransaction(Thread targetThread) {

			return threadTransactionTime.get(targetThread).get();
		}

		private void declareNeedForResource(Resource targetResource) {

			wantedResource.remove(Thread.currentThread());
			wantedResource.computeIfAbsent(Thread.currentThread(), 
										   (k) -> targetResource);
		}

		private void removeNeedForResource(Thread targetThread) {

			wantedResource.remove(targetThread);
		}

		private boolean isResourceWanted(Resource r) {

			for(ConcurrentMap.Entry<Thread, Resource> entry: wantedResource.entrySet()) {

				if(entry.getValue().getId().compareTo(r.getId()) == 0) {
					return true;
				}
			}

			return false;
		}

		/* true iff firstLong > secondLong (invoked only for different values) */
		private boolean compareLongs(long firstLong, long secondLong) {

			if(firstLong == Long.MIN_VALUE) {

				return false;
			}
			else if(secondLong == Long.MIN_VALUE) {

				return true;
			}
			else {

				return (firstLong > secondLong);
			}
		}

		private void checkInterrupt() throws InterruptedException {

			if(Thread.currentThread().isInterrupted()) {

				throw new InterruptedException();
			}
		}

		private void updateTransactionData(ResourceId targetId, ResourceOperation targetOperation) {

			threadOperations.computeIfAbsent(Thread.currentThread(), (k) -> new Stack<ResourceOperation>()).push(targetOperation);
			threadOperationsTargets.computeIfAbsent(Thread.currentThread(), (k) -> new Stack<ResourceId>()).push(targetId);

		}

		private void removeAttachmentToResources() {
			int count = 0;
			for(Resource r: resources) {
				if(resourceOwnership.get(r) == Thread.currentThread()) {
					count++;
				}
			}

			while(count > 0) {
				boolean acquiredMutex = false;
				if(Thread.currentThread().interrupted()) {}
				try {

					mutex.acquire();
					acquiredMutex = true;

					for(Resource r: resources) {

						if(resourceOwnership.get(r) == Thread.currentThread()) {

							count--;
							resourceOwnership.remove(r);
							boolean wait = isResourceWanted(r);

							if(wait) {

								waking.set(true);

								resourceAwait.get(r).release();

								while(waking.get()) {}
							}
							else {

								resourceAwait.get(r).release();
							}
						}
					}
				}
				catch(InterruptedException e) {
				}
				finally {

					if(acquiredMutex) {

						mutex.release();
					}
				}
			}
		}

		private void undoOperations() {

			Stack<ResourceOperation> operationsStack = threadOperations.
														   computeIfAbsent(Thread.currentThread(), 
														   (k) -> new Stack<ResourceOperation>());

			Stack<ResourceId> resourceIdStack = threadOperationsTargets.
													computeIfAbsent(Thread.currentThread(), 
													(k) -> new Stack<ResourceId>());

			while(!operationsStack.empty()) {

				ResourceOperation o = operationsStack.pop();
				ResourceId currentId = resourceIdStack.pop();

				for(Resource r: resources) {

					if(r.getId().compareTo(currentId) == 0) {

						o.undo(r);
					}
				}
			}
		}

		private Thread getThreadToInterrupt(Resource resourceReference) {

			boolean interrupt = false;
			/* obtain the thread which possesses the desired resource */
			Thread owner = resourceOwnership.get(resourceReference);

			/* initial value for the maximal value of transaction start time */
			long latestTime = getTimeOfTransaction(Thread.currentThread());

			/* thread which may have to be interrupted, initially current thread */
			Thread toInterruptIfNecessary = Thread.currentThread();

			while(owner != null) {

				if(abortedStatus.get(owner).get()) {

					interrupt = false;
					break;
				}
				/* cycle */
				if(owner == Thread.currentThread()) {

					interrupt = true;
					break;
				}		

				long currentOwnerTime = getTimeOfTransaction(owner);

				/* equal times, compare thread IDs */
				if(currentOwnerTime == latestTime &&
				   toInterruptIfNecessary.getId() < owner.getId()) {

					toInterruptIfNecessary = owner;
				}
				/* distinct times of transaction start */
				else if(compareLongs(currentOwnerTime, latestTime)) {

					toInterruptIfNecessary = owner;
				}

				Resource currentTarget = wantedResource.get(owner);
				owner = getOwnerOf(currentTarget);
			}	

			if(interrupt) {

				return toInterruptIfNecessary;
			}
			else {

				return null;
			}	
		}

		private void restoreDefaultTransactionStatus() {

			transactionStatus.computeIfAbsent(Thread.currentThread(), 
											  (k) -> new AtomicBoolean()).set(false);

			abortedStatus.computeIfAbsent(Thread.currentThread(), 
									      (k) -> new AtomicBoolean()).set(false);

			threadOperations.remove(Thread.currentThread());
			threadOperationsTargets.remove(Thread.currentThread());

		}

		public void startTransaction() throws AnotherTransactionActiveException {

			if(checkActiveTransaction()) {

				throw new AnotherTransactionActiveException();
			}

			setTransactionTime();
			setActiveTransactionStatus();
		}


		public void operateOnResourceInCurrentTransaction(ResourceId rid, 
														  ResourceOperation operation) 
														  throws 
														  ActiveTransactionAborted,
														  NoActiveTransactionException,
														  ResourceOperationException,
														  UnknownResourceIdException,
														  InterruptedException {

			if(!checkActiveTransaction()) {

				throw new NoActiveTransactionException();
			}

			if(checkAbortStatus()) {

				throw new ActiveTransactionAborted();
			}

			boolean checkPresence = false;
			boolean operationSuccess = true;

			Resource resourceReference = null;

			for(Resource r: this.resources) {

				if(r.getId().compareTo(rid) == 0) {

					resourceReference = r;
				}
			}

			if(resourceReference == null) {

				throw new UnknownResourceIdException(rid);
			}

			boolean acquiredMutex = false;
			boolean releasedMutex = false;

			if(resourceOwnership.get(resourceReference) != Thread.currentThread()) {

				try {

					mutex.acquire();
					acquiredMutex = true;

					Thread owner = resourceOwnership.computeIfAbsent(resourceReference, (k) -> Thread.currentThread());

					if(owner != Thread.currentThread()) {

						declareNeedForResource(resourceReference);
						Thread toInterrupt = getThreadToInterrupt(resourceReference);

						/* break cycle */
						if(toInterrupt != null) {

							setAbortedStatus(toInterrupt);
							removeNeedForResource(toInterrupt);

							toInterrupt.interrupt();
						}

						mutex.release();
						releasedMutex = true;

						resourceAwait.get(resourceReference).acquire();

						wantedResource.remove(Thread.currentThread());
						resourceOwnership.computeIfAbsent(resourceReference, (k) -> Thread.currentThread());

						if(waking.get()) {

							waking.set(false);
						}
					}
					else {

						resourceAwait.get(resourceReference).acquire();
						mutex.release();
						releasedMutex = true;
					}
				}
				catch(InterruptedException e) {

					throw e;
				}
				finally {

					wantedResource.remove(Thread.currentThread());
					if(acquiredMutex && !releasedMutex) {

						mutex.release();
					}
				}
			}

			try {

				operation.execute(resourceReference);
				checkInterrupt();
			}
			catch(ResourceOperationException |
				  InterruptedException e) {

				if(Thread.currentThread().interrupted()) {
					operation.undo(resourceReference);
				}

				operationSuccess = false;
				
				throw e;
			}
			finally {

				if(operationSuccess) {

					updateTransactionData(rid, operation);
				}
			}
		}

		public void commitCurrentTransaction() throws NoActiveTransactionException, 
													  ActiveTransactionAborted {

			if(!checkActiveTransaction()) {

				throw new NoActiveTransactionException();
			}

			if(checkAbortStatus()) {

				throw new ActiveTransactionAborted();
			}

			removeAttachmentToResources();
			restoreDefaultTransactionStatus();
		}

		public void rollbackCurrentTransaction() {

			if(checkActiveTransaction()) {

				undoOperations();
				removeAttachmentToResources();
				restoreDefaultTransactionStatus();
			}
		}

		public boolean isTransactionActive() {

			return checkActiveTransaction();
		}

		public boolean isTransactionAborted() {

			return (checkActiveTransaction() && checkAbortStatus());
		}
	}

	public final static TransactionManager newTM(
			Collection<Resource> resources,
			LocalTimeProvider timeProvider) {

		return new TM(resources, timeProvider);
	}
	
}
