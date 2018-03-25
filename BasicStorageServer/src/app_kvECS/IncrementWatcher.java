package app_kvECS;

import static common.zookeeper.ZKSession.FINISHED;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import common.zookeeper.ZKSession;

public class IncrementWatcher implements Watcher {

	private static final Logger log = Logger.getLogger(IncrementWatcher.class);

	private final String znode;
	private final ZKSession zkSession;
	private final AtomicInteger counter;
	private final int target;

	private boolean isCancelled = false;

	public IncrementWatcher(String znode, ZKSession zkSession, AtomicInteger counter, int target) {
		this.znode = znode;
		this.zkSession = zkSession;
		this.counter = counter;
		this.target = target;
	}

	public void cancel() {
		this.isCancelled = true;
	}

	@Override
	public void process(WatchedEvent event) {
		if (isCancelled) return;

		try {
			String status = zkSession.getNodeData(this.znode, this);
			if (Objects.equals(status, FINISHED)) {
				this.cancel();
				zkSession.updateNode(znode, new byte[0]);

				int counterValue = counter.incrementAndGet();
				if (counterValue == target) {
					synchronized (counter) {
						counter.notify();
					}
				}
			}
		} catch (KeeperException | InterruptedException e) {
			log.warn("Exception while reading server-ECS node", e);
		}

	}

}
