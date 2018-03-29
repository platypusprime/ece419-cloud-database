package testing;

import app_kvServer.KVServer;
import org.junit.Test;
import testing.util.ServerTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ReplicationTest extends ServerTest {

    @Test
    public void testSingleInsert() throws InterruptedException {
        server1.handlePutRequest("K1", "V1");

        // Wait for data to get replicated
        Thread.sleep(KVServer.REPLICATION_CHECK_INTERVAL + 4 * 1000);
        String val = server2.getKV("K1");

        assertEquals("V1", val);
    }

    @Test
    public void testSingleDelete() throws InterruptedException {
        // Insert data to both coordinator and replica
        server1.putKV("K2", "V2"); // Coordinator
        server2.putKV("K2", "V2"); // Replica

        if (!server1.getKV("K2").equals("V2"))
            throw new IllegalStateException("Server1 is missing recently added value");

        if (!server2.getKV("K2").equals("V2"))
            throw new IllegalStateException("Server2 is missing recently added value");

        // Delete key-value pair from coordinator
        server1.handlePutRequest("K2", null);

        // Wait for the delete to get propagated to the replica
        Thread.sleep(KVServer.REPLICATION_CHECK_INTERVAL + 4 * 1000);

        // Verify that the replica no longer contains the key
        assertNull(server2.getKV("K2"));
    }

    @Test
    public void testDuplicateInsert() throws InterruptedException {
        server1.handlePutRequest("K3", "V3");
        server1.handlePutRequest("K3", "V4");

        // Wait for data to get replicated
        Thread.sleep(KVServer.REPLICATION_CHECK_INTERVAL + 4 * 1000);
        String val = server2.getKV("K3");

        assertEquals("V4", val);
    }
}
