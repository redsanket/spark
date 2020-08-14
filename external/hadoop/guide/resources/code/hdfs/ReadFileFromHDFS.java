FileSystem fileSystem = FileSystem.get(conf);
Path path = new Path("/path/to/file.ext");
if (!fileSystem.exists(path)) {
  System.out.println("File does not exists");
  return;
}
FSDataInputStream in = fileSystem.open(path);
int numBytes = 0;
while ((numBytes = in.read(b))> 0) {
  // code to manipulate the data which is read
  System.out.prinln((char)numBytes));
}
in.close();
out.close();
fileSystem.close();