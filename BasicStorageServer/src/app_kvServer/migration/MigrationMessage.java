package app_kvServer.migration;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ecs.ECSNode;
import ecs.IECSNode;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class MigrationMessage {

    public enum Trigger {
        NODE_ADDITION("NODE_ADDITION"),
        NODE_DELETION("NODE_DELETION");

        private final String t;

        Trigger(String t) {
            this.t = t;
        }

        @Override
        public String toString() {
            return this.t;
        }
    }

    /**
     * The trigger that caused this migration operation (i.e. the addition of a node vs deletion of a node)
     */
    private Trigger trigger;

    /**
     * The source node (i.e. the one sending the data)
     */
    private ECSNode source;

    /**
     * The target node (i.e. the one receiving the data)
     */
    private ECSNode target;

    /**
     * The number of the current package (or chunk)
     */
    private int packageNum;

    /**
     * The number of remaining packages (or chunks)
     */
    private int remainingPackages;

    /**
     * The actual data (i.e. key-value pairs)
     */
    private Map<String, String> data;


    public MigrationMessage(Trigger trigger, ECSNode source, ECSNode target, int packageNum, int remainingPackages, Map<String, String> data) {
        this.trigger = trigger;
        this.source = source;
        this.target = target;
        this.packageNum = packageNum;
        this.remainingPackages = remainingPackages;
        this.data = Objects.requireNonNull(data);
    }

    public Map<String, String> getData() {
        return data;
    }

    public String toJSON() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public static MigrationMessage fromJSON(String data) {
        Gson gson = new Gson();
        return gson.fromJson(data, MigrationMessage.class);
    }
}
