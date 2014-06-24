package hadooptest.workflow.storm.topology.grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONValue;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class YidGrouping implements CustomStreamGrouping {

        private List<Integer> _tasks;

        @Override
        public List<Integer> chooseTasks(int taskId, List<Object> values) {
            // parse out the yid from the values. use it to decide where to route
            String args = (String) values.get(0);
            Map<String, Object> v;
            v = (Map<String, Object>) JSONValue.parse(args);
            Object yid = v.get("login");
            int index = (yid == null) ? 0 : Math.abs(yid.hashCode())
                    % _tasks.size();
            List<Integer> tasks = new ArrayList<Integer>();
            tasks.add(_tasks.get(index));
            return tasks;
        }

        @Override
        public void prepare(WorkerTopologyContext context, GlobalStreamId streamId,
                        List<Integer> tasks) {
                this._tasks = tasks;
        }

}
