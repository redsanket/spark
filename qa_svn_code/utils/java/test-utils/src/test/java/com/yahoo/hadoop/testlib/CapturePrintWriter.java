package com.yahoo.hadoop.testlib;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import static com.google.common.base.Preconditions.*;

/**
 * Used to capture the output to System.out
 */
public class CapturePrintWriter extends PrintStream {
  private ByteArrayOutputStream bufferOutputStream;

  public CapturePrintWriter() {
    this(new ByteArrayOutputStream());
  }
  public CapturePrintWriter(ByteArrayOutputStream outputStream) {
    super(checkNotNull(outputStream, "OutputStream can not be null"));
    this.bufferOutputStream = outputStream;
  }

  /**
   * This method will return data from the output stream that is unread
   */
  public String getUnreadString() throws IOException {
    bufferOutputStream.flush();
    String output = new String(bufferOutputStream.toByteArray());
    bufferOutputStream.reset();
    return output.trim();
  }
}
