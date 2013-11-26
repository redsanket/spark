import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Symlink extends Configured implements Tool {

	public Symlink() {
	}

	public int ln_s(Path target, Path symlink) throws org.apache.hadoop.fs.UnsupportedFileSystemException {

		FileContext fc = FileContext.getFileContext(getConf());
		try {
			fc.createSymlink(target, symlink, false);
		} catch (java.io.IOException e) {
			System.out.println(e);
			return -1;
		}

		return 0;
	}

	public int ls_l(Path path) throws java.io.IOException, java.io.FileNotFoundException {

		FileSystem fs = path.getFileSystem(getConf());
		FileStatus[] stats = fs.listStatus(path);
		for (FileStatus stat: stats) {
			Path cur = stat.getPath();

			System.out.print((stat.isDirectory() ? "d" : (stat.isSymlink() ? "l" : "-")) +
					stat.getPermission() + " " +
					stat.getOwner() + " " +
					stat.getGroup() + " ");
			System.out.println(cur.toUri().getPath() + (stat.isSymlink() ? " -> " + stat.getSymlink(): ""));
		}

		return 0;
	}

	public int run(String argv[]) throws Exception {

		if (argv.length < 1) {
			return -1;
		}

		int i = 0;
		String cmd = argv[i++];
		if ("ln".equals(cmd)) {
			if (argv.length < 3) {
				return -1;
			}

			Path target = new Path(argv[i++]);
			Path symlink = new Path(argv[i++]);
			return ln_s(target, symlink);
		} else if ("ls".equals(cmd)) {
			if (argv.length < 2) {
				return -1;
			}

			Path file = new Path(argv[i++]);
			return ls_l(file);
		}

		return -1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Symlink(), args);
		System.exit(ret);
	}
}
