package hadooptest.tez.ats;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

public class SeedData {
	public String appId;
	public List<DAG> dags;
	public String appStartedByUser;

	public SeedData() {
		dags = new ArrayList<DAG>();
	}

	public DAG getDagObject(String dagName) {
		for (DAG searchedDag : dags) {
			if (searchedDag.name.equals(dagName)) {
				return searchedDag;
			}
		}
		return null;
	}

	public static class DAG {
		public String name;
		public String id;
		public List<Vertex> vertices;

		public DAG() {
			vertices = new ArrayList<Vertex>();
		}

		public Vertex getVertexObject(String vertexName) {
			for (Vertex searchedVertex : vertices) {
				if (searchedVertex.name.equals(vertexName)) {
					return searchedVertex;
				}
			}
			return null;
		}

		public static class Vertex {
			public String name;
			public String id;
			public List<Task> tasks;

			public Vertex() {
				tasks = new ArrayList<Task>();
			}

			public static class Task {
				public String id;
				public List<Attempt> attempts;

				public Task() {
					attempts = new ArrayList<Attempt>();
				}

				public static class Attempt {
					public String id;

					void dump() {
						TestSession.logger.info("AttemptId: [id:" + id + "]");
					}
				}

				void dump() {
					TestSession.logger.info("TASK [id:" + id + "]");
					for (Attempt anAttempt : attempts) {
						anAttempt.dump();
					}
				}

			}

			public void dump() {
				TestSession.logger.info("Vertex [name:" + name + "] id:[" + id
						+ "]");
				for (Task aTask : tasks) {
					aTask.dump();
				}
			}
		}

		public void dump() {
			TestSession.logger.info("DAG [name:" + name + "] id:[" + id + "]");
			for (Vertex aVertex : vertices) {
				aVertex.dump();
			}
		}
	}

	public void dump() {
		TestSession.logger.info("AppId:" + appId);
		TestSession.logger.info("AppStartedByUser:" + appStartedByUser);
		for (DAG aDag : dags) {
			aDag.dump();
		}
	}

}
