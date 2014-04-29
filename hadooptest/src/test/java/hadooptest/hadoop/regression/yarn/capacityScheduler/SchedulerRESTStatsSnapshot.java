package hadooptest.hadoop.regression.yarn.capacityScheduler;

import java.util.ArrayList;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

enum QueueType {
	PARENT, LEAF
};

public class SchedulerRESTStatsSnapshot {
	public ParentQueue rootQueue;
	public ArrayList<LeafQueue> allLeafQueues = new ArrayList<LeafQueue>();

	public SchedulerRESTStatsSnapshot(JSONObject jsonObject) {

		try {
			JSONObject schedulerLayer = (JSONObject) jsonObject
					.get("scheduler");
			JSONObject schedulerInfoLayer = (JSONObject) schedulerLayer
					.get("schedulerInfo");
			rootQueue = new ParentQueue();
			rootQueue.queueName = schedulerInfoLayer.getString("queueName");
			rootQueue.capacity = schedulerInfoLayer.getDouble("capacity");
			rootQueue.usedCapacity = schedulerInfoLayer
					.getDouble("usedCapacity");
			rootQueue.maxCapacity = schedulerInfoLayer.getDouble("maxCapacity");

			JSONObject queuesJsonLayer = (JSONObject) schedulerInfoLayer
					.get("queues");
			recursivelyUpdateQueueStats(rootQueue, queuesJsonLayer);

		} catch (JSONException e) {
			e.printStackTrace();
		}

	}

	private void recursivelyUpdateQueueStats(ParentQueue parentQueue,
			JSONObject queuesJsonLayer) throws JSONException {
		JSONArray queueArrayJson = (JSONArray) queuesJsonLayer.get("queue");
		for (int xx = 0; xx < queueArrayJson.size(); xx++) {
			if (queueArrayJson.getJSONObject(xx).has("type")) {
				LeafQueue aLeafQueue = new LeafQueue(
						queueArrayJson.getJSONObject(xx));
				parentQueue.addQueue(aLeafQueue, QueueType.LEAF);
			} else {
				// Is parent
				ParentQueue newParent = new ParentQueue();
				parentQueue.addQueue(newParent, QueueType.PARENT);
				newParent.capacity = queueArrayJson.getJSONObject(xx)
						.getDouble("capacity");
				newParent.usedCapacity = queueArrayJson.getJSONObject(xx)
						.getDouble("usedCapacity");
				newParent.maxCapacity = queueArrayJson.getJSONObject(xx)
						.getDouble("maxCapacity");
				newParent.queueName = queueArrayJson.getJSONObject(xx)
						.getString("queueName");
				if (queueArrayJson.getJSONObject(xx).has("absoluteCapacity")) {
					newParent.absoluteCapacity = queueArrayJson.getJSONObject(
							xx).getDouble("absoluteCapacity");
				}
				if (queueArrayJson.getJSONObject(xx).has("absoluteMaxCapacity")) {
					newParent.absoluteMaxCapacity = queueArrayJson
							.getJSONObject(xx).getDouble("absoluteMaxCapacity");
				}
				if (queueArrayJson.getJSONObject(xx)
						.has("absoluteUsedCapacity")) {
					newParent.absoluteUsedCapacity = queueArrayJson
							.getJSONObject(xx)
							.getDouble("absoluteUsedCapacity");
				}
				if (queueArrayJson.getJSONObject(xx).has("numApplications")) {
					newParent.numApplications = queueArrayJson
							.getJSONObject(xx).getInt("numApplications");
				}
				if (queueArrayJson.getJSONObject(xx).has("state")) {
					newParent.state = queueArrayJson.getJSONObject(xx)
							.getString("state");
				}
				JSONObject tempQueuesLayer = queueArrayJson.getJSONObject(xx)
						.getJSONObject("queues");
				recursivelyUpdateQueueStats(newParent, tempQueuesLayer);

			}
		}

	}

	public class ParentQueue {
		public String queueName;
		public double capacity;
		public double usedCapacity;
		public double maxCapacity;
		public double absoluteCapacity;
		public double absoluteMaxCapacity;
		public double absoluteUsedCapacity;
		public int numApplications;
		public String state;

		public ArrayList<ParentQueue> parentQueues = new ArrayList<ParentQueue>();
		public ArrayList<LeafQueue> leafQueues = new ArrayList<LeafQueue>();

		public void addQueue(Object queue, QueueType queueType) {
			if (queueType == QueueType.PARENT) {
				parentQueues.add((ParentQueue) queue);
			} else if (queueType == QueueType.LEAF) {
				leafQueues.add((LeafQueue) queue);
			}

		}

		/**
		 * Since the queues can/will be nested and each level can again, either
		 * be a leaf queue or a parent queue just collate the leaf queue info,
		 * 'cos thats all that we are interested in. This method collates all
		 * the leaf queue info and maintains it as an list, for easier processing
		 * for the outside world.
		 */
		public void recursivelyAssimilateLeafQueues() {
			recursivelyAssimilateLeafQueues(this);
		}

		void recursivelyAssimilateLeafQueues(ParentQueue pq) {
			for (LeafQueue aLeafQueue : pq.leafQueues) {
				// System.out.println(aLeafQueue);
				allLeafQueues.add(aLeafQueue);
			}
			for (ParentQueue aParentQueue : pq.parentQueues) {
				recursivelyAssimilateLeafQueues(aParentQueue);
				// System.out.println("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");
				// System.out.println("ParentQueue queueName:" + queueName);
				// System.out.println("ParentQueue capacity:" + capacity);
				// System.out.println("ParentQueue usedCapacity:" +
				// usedCapacity);
				// System.out.println("ParentQueue maxCapacity:" + maxCapacity);
				// System.out.println("ParentQueue absoluteCapacity:" +
				// absoluteCapacity);
				// System.out.println("ParentQueue absoluteMaxCapacity:" +
				// absoluteMaxCapacity);
				// System.out.println("ParentQueue absoluteUsedCapacity:" +
				// absoluteUsedCapacity);
				// System.out.println("ParentQueue numApplications:" +
				// numApplications);
				// System.out.println("ParentQueue state:" + state);
				// System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");

			}
		}

	}

	public class LeafQueue {
		public String queueName;
		public String state;
		public double capacity;
		public double usedCapacity;
		public double maxCapacity;
		public double absoluteCapacity;
		public double absoluteMaxCapacity;
		public double absoluteUsedCapacity;
		public int numApplications;
		public ResourcesUsed resourcesUsed;
		public int numActiveApplications;
		public int numPendingApplications;
		public int numContainers;
		public int maxApplications;
		public int maxApplicationsPerUser;
		public int maxActiveApplications;
		public int maxActiveApplicationsPerUser;
		public int userLimit;
		public ArrayList<User> users;
		public double userLimitFactor;

		LeafQueue(JSONObject consume) throws JSONException {

			this.capacity = consume.getDouble("capacity");
			// System.out.println("Added LeafQueue.capacity=" + capacity);
			this.usedCapacity = consume.getDouble("usedCapacity");
			// System.out.println("Added LeafQueue.usedCapacity=" + capacity);
			this.maxCapacity = consume.getDouble("maxCapacity");
			// System.out.println("Added LeafQueue.maxCapacity=" + maxCapacity);
			this.absoluteCapacity = consume.getDouble("absoluteCapacity");
			// System.out.println("Added LeafQueue.absoluteCapacity=" +
			// absoluteCapacity);
			this.absoluteMaxCapacity = consume.getDouble("absoluteMaxCapacity");
			// System.out.println("Added LeafQueue.absoluteMaxCapacity=" +
			// absoluteMaxCapacity);
			this.absoluteUsedCapacity = consume
					.getDouble("absoluteUsedCapacity");
			// System.out.println("Added LeafQueue.absoluteUsedCapacity=" +
			// absoluteUsedCapacity);
			this.numApplications = consume.getInt("numApplications");
			// System.out.println("Added LeafQueue.numApplications=" +
			// numApplications);
			this.queueName = consume.getString("queueName");
			// System.out.println("Added LeafQueue.queueName=" + queueName);
			this.state = consume.getString("state");
			// System.out.println("Added LeafQueue.state=" + state);
			resourcesUsed = new ResourcesUsed(
					consume.getJSONObject("resourcesUsed"));
			this.numActiveApplications = consume
					.getInt("numActiveApplications");
			// System.out.println("Added LeafQueue.numActiveApplications=" +
			// numActiveApplications);
			this.numPendingApplications = consume
					.getInt("numPendingApplications");
			// System.out.println("Added LeafQueue.numPendingApplications=" +
			// numPendingApplications);
			this.numContainers = consume.getInt("numContainers");
			// System.out.println("Added LeafQueue.numContainers=" +
			// numContainers);
			this.maxApplications = consume.getInt("maxApplications");
			// System.out.println("Added LeafQueue.maxApplications=" +
			// maxApplications);
			this.maxApplicationsPerUser = consume
					.getInt("maxApplicationsPerUser");
			// System.out.println("Added LeafQueue.maxApplicationsPerUser=" +
			// maxApplicationsPerUser);
			this.maxActiveApplications = consume
					.getInt("maxActiveApplications");
			// System.out.println("Added LeafQueue.maxActiveApplications=" +
			// maxActiveApplications);
			this.maxActiveApplicationsPerUser = consume
					.getInt("maxActiveApplicationsPerUser");
			// System.out.println("Added LeafQueue.maxActiveApplicationsPerUser="
			// + maxActiveApplicationsPerUser);
			this.userLimit = consume.getInt("userLimit");
			// System.out.println("Added LeafQueue.userLimit=" + userLimit);
			this.userLimitFactor = consume.getInt("userLimitFactor");
			// System.out.println("Added LeafQueue.userLimitFactor=" +
			// userLimitFactor);

			users = new ArrayList<User>();
			if (consume.has("users") && (!consume.get("users").equals("null"))) {
				// System.out.println("USers is " + consume.get("users")+
				// " for queue:" + queueName);
				JSONObject usersLayer = consume.getJSONObject("users");
				JSONArray jsonUsersArray = usersLayer.getJSONArray("user");
				for (int jj = 0; jj < jsonUsersArray.size(); jj++) {
					User aUser = new User(jsonUsersArray.getJSONObject(jj));
					users.add(aUser);
				}
			} else {
				// System.out.println("USers is empty for queue:" + queueName);
			}
			// System.out.println("---------------------------");
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("LeafQueue queueName:" + queueName + "\n");
			sb.append("LeafQueue state:" + state + "\n");
			sb.append("LeafQueue capacity:" + capacity + "\n");
			sb.append("LeafQueue usedCapacity:" + usedCapacity + "\n");
			sb.append("LeafQueue maxCapacity:" + maxCapacity + "\n");
			sb.append("LeafQueue absoluteCapacity:" + absoluteCapacity + "\n");
			sb.append("LeafQueue absoluteMaxCapacity:" + absoluteMaxCapacity
					+ "\n");
			sb.append("LeafQueue numApplications:" + numApplications + "\n");
			sb.append("LeafQueue absoluteUsedCapacity:" + absoluteUsedCapacity
					+ "\n");
			sb.append("LeafQueue resourcesUsed:" + resourcesUsed + "\n");
			sb.append("LeafQueue numActiveApplications:"
					+ numActiveApplications + "\n");
			sb.append("LeafQueue numPendingApplications:"
					+ numPendingApplications + "\n");
			sb.append("LeafQueue numContainers:" + numContainers + "\n");
			sb.append("LeafQueue maxApplications:" + maxApplications + "\n");
			sb.append("LeafQueue maxApplicationsPerUser:"
					+ maxApplicationsPerUser + "\n");
			sb.append("LeafQueue maxActiveApplications:"
					+ maxActiveApplications + "\n");
			sb.append("LeafQueue maxActiveApplicationsPerUser:"
					+ maxActiveApplicationsPerUser + "\n");
			sb.append("LeafQueue userLimit:" + userLimit + "\n");

			for (User aUser : users) {
				sb.append("LeafQueue User:" + aUser);
			}
			sb.append("LeafQueue userLimitFactor:" + userLimitFactor);
			sb.append("\n=====================================\n");
			return sb.toString();

		}

	}

	class User {
		String userName;
		ResourcesUsed resourcesUsed;
		int numPendingApplications;
		int numActiveApplications;

		User(JSONObject userJson) throws JSONException {
			this.userName = userJson.getString("username");
			// System.out.println("Added user.userName=" + userName);
			this.numPendingApplications = userJson
					.getInt("numPendingApplications");
			// System.out.println("Added user.numPendingApplications=" +
			// numPendingApplications);
			this.numActiveApplications = userJson
					.getInt("numActiveApplications");
			// System.out.println("Added user.numActiveApplications=" +
			// numActiveApplications);
			this.resourcesUsed = new ResourcesUsed(
					userJson.getJSONObject("resourcesUsed"));
			// System.out.println("Added user.resourcesUsed=" + resourcesUsed);
		}

		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("\tUser username:" + userName + "\n");
			sb.append("\tUser resourcesUsed:" + resourcesUsed + "\n");
			sb.append("\tUser numPendingApplications:" + numPendingApplications
					+ "\n");
			sb.append("\tUser numActiveApplications:" + numActiveApplications
					+ "\n");
			sb.append("-------------------------------------\n");
			return sb.toString();
		}
	}

	class ResourcesUsed {
		int memory;
		int cores;

		ResourcesUsed(JSONObject consume) throws JSONException {
			this.memory = consume.getInt("memory");
			// System.out.println("Added resourcesUsed.memeory=" + memory);
			this.cores = consume.getInt("vCores");
			// System.out.println("Added resourcesUsed.cores=" + cores);
		}

		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("\n\tResourceUsed memory: " + memory + "\n");
			sb.append("\tResourceUsed vCores: " + cores + "\n");
			sb.append(".......................................\n");
			return sb.toString();
		}
	}

}
