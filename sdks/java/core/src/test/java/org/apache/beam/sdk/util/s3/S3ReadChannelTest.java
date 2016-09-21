package org.apache.beam.sdk.util.s3;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.S3Options;
import org.apache.beam.sdk.util.S3IOChannelFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests of GcsPath.
 */
@RunWith(JUnit4.class)
public class S3ReadChannelTest {

  private S3IOChannelFactory factory;

  @Before
  public void setUp() {
    factory = new S3IOChannelFactory(PipelineOptionsFactory.as(S3Options.class));
  }

  /*
  private static S3Options OptionsWithTestCredential() {
    S3Options pipelineOptions = PipelineOptionsFactory.as(S3Options.class);
    // TODO: add credentials for AWS
    // pipelineOptions.setGcpCredential(new TestCredential());
    return pipelineOptions;
  }
  */

  @Test
  public void testReadBuffer() throws Exception { }
}
