package com.yahoo.hadoop;

import com.google.common.io.Files;
import com.yahoo.hadoop.testlib.CapturePrintWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URISyntaxException;

import static org.testng.Assert.*;

public class SequenceFileUtilTests {
  private static final Log LOG = LogFactory.getLog(SequenceFileUtilTests.class.getName());

  private FileSystem fs = null;
  private Configuration conf = null;
  private File tmpDir = null;


  @BeforeClass(alwaysRun = true)
  public void init() throws URISyntaxException, IOException {
    tmpDir = Files.createTempDir();
    conf = new Configuration();
    fs = new LocalFileSystem();
    fs.initialize(tmpDir.toURI(), conf);

    LOG.info("Tmp file dir " + tmpDir);
    LOG.info("FileSystem URI" + fs.getUri());
  }

  // Method tests
  @Test(groups = {"Integration"})
  public void getCompressionCodec() throws IOException {
    Path sequenceFile = new Path(tmpDir.getAbsolutePath(),
        "seq" + System.nanoTime());
    Configuration testConf = new Configuration(conf);

    SequenceFile.getDefaultCompressionType(testConf);


    SequenceFile.Writer writer = SequenceFile.createWriter(fs, testConf,
        sequenceFile, Text.class, Text.class);

    writer.append(new Text("Hello"), new Text("World"));
    writer.close();

    assertTrue(fs.exists(sequenceFile),
        "Sequence file doesnt exist " + sequenceFile);

    SequenceFile.CompressionType type =
        SequenceFileUtil.getCompressionType(testConf, sequenceFile);
    assertEquals(type, SequenceFile.CompressionType.RECORD);

    CompressionCodec codec =
        SequenceFileUtil.getCompressionCodec(testConf, sequenceFile);
    assertEquals(codec.getClass(), DefaultCodec.class);
  }

  @Test(groups = {"Integration"})
  public void getCompressionCodecNoCompression() throws IOException {
    Path sequenceFile = new Path(tmpDir.getAbsolutePath(),
        "seq" + System.nanoTime());
    Configuration testConf = new Configuration(conf);

    testConf.setEnum("io.seqfile.compression.type",
        SequenceFile.CompressionType.NONE);


    SequenceFile.Writer writer = SequenceFile.createWriter(fs, testConf,
        sequenceFile, Text.class, Text.class);

    writer.append(new Text("Hello"), new Text("World"));
    writer.close();

    assertTrue(fs.exists(sequenceFile),
        "Sequence file doesnt exist " + sequenceFile);

    SequenceFile.CompressionType type =
        SequenceFileUtil.getCompressionType(testConf, sequenceFile);
    assertEquals(type, SequenceFile.CompressionType.NONE);

    CompressionCodec codec =
        SequenceFileUtil.getCompressionCodec(testConf, sequenceFile);
    assertNull(codec, "There was a CompressionCodec defined");
  }

  @Test(groups = {"Integration"})
  public void getCompressionCodecRecordType() throws IOException {
    Path sequenceFile = new Path(tmpDir.getAbsolutePath(),
        "seq" + System.nanoTime());
    Configuration testConf = new Configuration(conf);

    testConf.setEnum("io.seqfile.compression.type",
        SequenceFile.CompressionType.RECORD);


    SequenceFile.Writer writer = SequenceFile.createWriter(fs, testConf,
        sequenceFile, Text.class, Text.class);

    writer.append(new Text("Hello"), new Text("World"));
    writer.close();

    assertTrue(fs.exists(sequenceFile),
        "Sequence file doesnt exist " + sequenceFile);

    SequenceFile.CompressionType type =
        SequenceFileUtil.getCompressionType(testConf, sequenceFile);
    assertEquals(type, SequenceFile.CompressionType.RECORD);

    CompressionCodec codec =
        SequenceFileUtil.getCompressionCodec(testConf, sequenceFile);
    assertEquals(codec.getClass(), DefaultCodec.class);
  }

  @Test(groups = {"Integration"})
  public void getCompressionCodecBlockType() throws IOException {
    Path sequenceFile = new Path(tmpDir.getAbsolutePath(),
        "seq" + System.nanoTime());
    Configuration testConf = new Configuration(conf);

    testConf.setEnum("io.seqfile.compression.type",
        SequenceFile.CompressionType.BLOCK);


    SequenceFile.Writer writer = SequenceFile.createWriter(fs, testConf,
        sequenceFile, Text.class, Text.class);

    writer.append(new Text("Hello"), new Text("World"));
    writer.close();

    assertTrue(fs.exists(sequenceFile),
        "Sequence file doesnt exist " + sequenceFile);

    SequenceFile.CompressionType type =
        SequenceFileUtil.getCompressionType(testConf, sequenceFile);
    assertEquals(type, SequenceFile.CompressionType.BLOCK);

    CompressionCodec codec =
        SequenceFileUtil.getCompressionCodec(testConf, sequenceFile);
    assertEquals(codec.getClass(), DefaultCodec.class);
  }

  @Test(groups = {"Integration"})
  public void getCompressionCodecBZip2() throws IOException {
    Path sequenceFile = new Path(tmpDir.getAbsolutePath(),
        "seq" + System.nanoTime());
    Configuration testConf = new Configuration(conf);

    SequenceFile.Writer writer = SequenceFile.createWriter(testConf,
        SequenceFile.Writer.file(sequenceFile),
        SequenceFile.Writer.keyClass(Text.class),
        SequenceFile.Writer.valueClass(Text.class),
        SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK,
            new BZip2Codec())
    );


    writer.append(new Text("Hello"), new Text("World"));
    writer.close();

    assertTrue(fs.exists(sequenceFile),
        "Sequence file doesnt exist " + sequenceFile);

    SequenceFile.CompressionType type =
        SequenceFileUtil.getCompressionType(testConf, sequenceFile);
    assertEquals(type, SequenceFile.CompressionType.BLOCK);

    CompressionCodec codec =
        SequenceFileUtil.getCompressionCodec(testConf, sequenceFile);
    assertEquals(codec.getClass(), BZip2Codec.class);
  }

  // Main method tests
  @Test(groups = {"Integration"})
  public void getCompressionCodecMain() throws IOException {
    Path sequenceFile = new Path(tmpDir.getAbsolutePath(),
        "seq" + System.nanoTime());
    Configuration testConf = new Configuration(conf);

    SequenceFile.getDefaultCompressionType(testConf);


    SequenceFile.Writer writer = SequenceFile.createWriter(fs, testConf,
        sequenceFile, Text.class, Text.class);

    writer.append(new Text("Hello"), new Text("World"));
    writer.close();

    assertTrue(fs.exists(sequenceFile),
        "Sequence file doesnt exist " + sequenceFile);


    // save output stream for later
    PrintStream sysOut = System.out;
    try {
      CapturePrintWriter bufferOut = new CapturePrintWriter();
      System.setOut(bufferOut);

      // writes to std out
      SequenceFileUtil.main(new String[] {"getCompressionType", sequenceFile.toString()});

      // as extra white space, so remove
      assertEquals(bufferOut.getUnreadString(), SequenceFile.CompressionType.RECORD.toString());

      SequenceFileUtil.main(new String[] {"getCompressionCodec", sequenceFile.toString()});

      assertEquals(bufferOut.getUnreadString(), DefaultCodec.class.getName());
    } finally {
      // restore output stream
      System.setOut(sysOut);
    }
  }

  @Test(groups = {"Integration"})
  public void getCompressionCodecNoCompressionMain() throws IOException {
    Path sequenceFile = new Path(tmpDir.getAbsolutePath(),
        "seq" + System.nanoTime());
    Configuration testConf = new Configuration(conf);

    testConf.setEnum("io.seqfile.compression.type",
        SequenceFile.CompressionType.NONE);


    SequenceFile.Writer writer = SequenceFile.createWriter(fs, testConf,
        sequenceFile, Text.class, Text.class);

    writer.append(new Text("Hello"), new Text("World"));
    writer.close();

    assertTrue(fs.exists(sequenceFile),
        "Sequence file doesnt exist " + sequenceFile);


    // save output stream for later
    PrintStream sysOut = System.out;
    try {
      CapturePrintWriter bufferOut = new CapturePrintWriter();
      System.setOut(bufferOut);

      // writes to std out
      SequenceFileUtil.main(new String[] {"getCompressionType", sequenceFile.toString()});

      // as extra white space, so remove
      assertEquals(bufferOut.getUnreadString(), SequenceFile.CompressionType.NONE.toString());

      SequenceFileUtil.main(new String[] {"getCompressionCodec", sequenceFile.toString()});

      assertEquals(bufferOut.getUnreadString(), "");
    } finally {
      // restore output stream
      System.setOut(sysOut);
    }
  }

  @Test(groups = {"Integration"})
  public void getCompressionCodecRecordTypeMain() throws IOException {
    Path sequenceFile = new Path(tmpDir.getAbsolutePath(),
        "seq" + System.nanoTime());
    Configuration testConf = new Configuration(conf);

    testConf.setEnum("io.seqfile.compression.type",
        SequenceFile.CompressionType.RECORD);


    SequenceFile.Writer writer = SequenceFile.createWriter(fs, testConf,
        sequenceFile, Text.class, Text.class);

    writer.append(new Text("Hello"), new Text("World"));
    writer.close();

    assertTrue(fs.exists(sequenceFile),
        "Sequence file doesnt exist " + sequenceFile);


    // save output stream for later
    PrintStream sysOut = System.out;
    try {
      CapturePrintWriter bufferOut = new CapturePrintWriter();
      System.setOut(bufferOut);

      // writes to std out
      SequenceFileUtil.main(new String[] {"getCompressionType", sequenceFile.toString()});

      // as extra white space, so remove
      assertEquals(bufferOut.getUnreadString(), SequenceFile.CompressionType.RECORD.toString());

      SequenceFileUtil.main(new String[] {"getCompressionCodec", sequenceFile.toString()});

      assertEquals(bufferOut.getUnreadString(), DefaultCodec.class.getName());
    } finally {
      // restore output stream
      System.setOut(sysOut);
    }
  }

  @Test(groups = {"Integration"})
  public void getCompressionCodecBlockTypeMain() throws IOException {
    Path sequenceFile = new Path(tmpDir.getAbsolutePath(),
        "seq" + System.nanoTime());
    Configuration testConf = new Configuration(conf);

    testConf.setEnum("io.seqfile.compression.type",
        SequenceFile.CompressionType.BLOCK);


    SequenceFile.Writer writer = SequenceFile.createWriter(fs, testConf,
        sequenceFile, Text.class, Text.class);

    writer.append(new Text("Hello"), new Text("World"));
    writer.close();

    assertTrue(fs.exists(sequenceFile),
        "Sequence file doesnt exist " + sequenceFile);

    // save output stream for later
    PrintStream sysOut = System.out;
    try {
      CapturePrintWriter bufferOut = new CapturePrintWriter();
      System.setOut(bufferOut);

      // writes to std out
      SequenceFileUtil.main(new String[] {"getCompressionType", sequenceFile.toString()});

      // as extra white space, so remove
      assertEquals(bufferOut.getUnreadString(), SequenceFile.CompressionType.BLOCK.toString());

      SequenceFileUtil.main(new String[] {"getCompressionCodec", sequenceFile.toString()});

      assertEquals(bufferOut.getUnreadString(), DefaultCodec.class.getName());
    } finally {
      // restore output stream
      System.setOut(sysOut);
    }
  }

  @Test(groups = {"Integration"})
  public void getCompressionCodecBZip2Main() throws IOException {
    Path sequenceFile = new Path(tmpDir.getAbsolutePath(),
        "seq" + System.nanoTime());
    Configuration testConf = new Configuration(conf);

    SequenceFile.Writer writer = SequenceFile.createWriter(testConf,
        SequenceFile.Writer.file(sequenceFile),
        SequenceFile.Writer.keyClass(Text.class),
        SequenceFile.Writer.valueClass(Text.class),
        SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK,
            new BZip2Codec())
    );


    writer.append(new Text("Hello"), new Text("World"));
    writer.close();

    assertTrue(fs.exists(sequenceFile),
        "Sequence file doesnt exist " + sequenceFile);

    // save output stream for later
    PrintStream sysOut = System.out;
    try {
      CapturePrintWriter bufferOut = new CapturePrintWriter();
      System.setOut(bufferOut);

      // writes to std out
      SequenceFileUtil.main(new String[] {"getCompressionType", sequenceFile.toString()});

      // as extra white space, so remove
      assertEquals(bufferOut.getUnreadString(), SequenceFile.CompressionType.BLOCK.toString());

      SequenceFileUtil.main(new String[] {"getCompressionCodec", sequenceFile.toString()});

      assertEquals(bufferOut.getUnreadString(), BZip2Codec.class.getName());
    } finally {
      // restore output stream
      System.setOut(sysOut);
    }
  }
}
