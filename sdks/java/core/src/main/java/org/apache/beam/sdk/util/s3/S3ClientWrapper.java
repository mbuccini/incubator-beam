/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.util.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.S3Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around the S3 client to provide operations on Amazon S3.
 */
public class S3ClientWrapper {

  /**
   * This is a {@link DefaultValueFactory} able to create a {@link S3ClientWrapper} using
   * any transport flags specified on the {@link PipelineOptions}.
   */
  public static class S3ClientWrapperFactory implements DefaultValueFactory<S3ClientWrapper> {

    @Override
    public S3ClientWrapper create(PipelineOptions options) {
      // LOG.debug("Creating new S3ClientWrapper");
      S3Options s3options = options.as(S3Options.class);
      AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
      return new S3ClientWrapper(s3Client, s3options.getJobName());
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(S3ClientWrapper.class);

  private final AmazonS3 s3client;


  private S3ClientWrapper(AmazonS3 s3client, String jobName) {
    this.s3client = s3client;
  }


  /**
   * Returns the file size from S3 or {@link FileNotFoundException}
   * if the resource does not exist.
   */
  public long fileSize(AmazonS3URI path) {
    return -1;
  }

  /**
   * Opens an object in AWS S3.
   *
   * @param path the S3 filename to read from
   * @return a SeekableByteChannel that can read the object data
   */
  public ReadableByteChannel open(AmazonS3URI path) throws IOException {

    // TODO: a wrapper needs to be created here so that it's possible to
    // read an object as a channel
      return new S3ReadChannel(s3client, path.getBucket(), path.getKey());
  }
}
