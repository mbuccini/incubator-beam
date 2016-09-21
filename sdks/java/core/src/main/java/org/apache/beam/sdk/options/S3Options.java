package org.apache.beam.sdk.options;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.beam.sdk.util.s3.S3ClientWrapper;

/**
 * Options used to configure Amazon S3.
 */
public interface S3Options extends PipelineOptions {

  /**
   * The S3ClientWrapper instance that should be used to communicate with Amazon AWS.
   */
  @JsonIgnore
  @Description("The S3 client that should be used to communicate with Amazon AWS.")
  @Default.InstanceFactory(S3ClientWrapper.S3ClientWrapperFactory.class)
  @Hidden
  S3ClientWrapper getS3ClientWrapper();
  void setS3ClientWrapper(S3ClientWrapper value);

}
