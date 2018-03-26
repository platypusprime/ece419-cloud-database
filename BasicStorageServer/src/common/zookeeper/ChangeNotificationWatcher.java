package common.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * A watcher that simply sends a notification to an object's monitor when it is
 * triggered. Can be cancelled beforehand.
 */
public class ChangeNotificationWatcher implements Watcher {

	private Object o;
	private boolean isCancelled = false;

	/**
	 * Creates a watcher that will notify the given object's monitor.
	 * 
	 * @param o The object whose monitor is to be notified
	 */
	public ChangeNotificationWatcher(Object o) {
		this.o = o;
	}

	/**
	 * Prevents this monitor from notifying its object if it is triggered in the
	 * future.
	 */
	public void cancel() {
		this.isCancelled = true;
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println("REEEE" + o.toString());

		if (isCancelled) return;

		synchronized (o) {
			o.notify();
		}
	}

}