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
package org.apache.beam.sdk.util;

import com.amazonaws.services.s3.AmazonS3URI;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;

import org.apache.beam.sdk.options.S3Options;
import org.apache.beam.sdk.util.s3.S3ClientWrapper;

/**
 * Implements IOChannelFactory for S3.
 */
public final class S3IOChannelFactory implements IOChannelFactory {

  private final S3Options options;

  public S3IOChannelFactory(final S3Options options){
    this.options = options;
  }

  @Override
  public Collection<String> match(String spec) throws IOException {
      return null;
  }

  @Override
  public ReadableByteChannel open(String spec) throws IOException {
    AmazonS3URI path = new AmazonS3URI(spec);
    S3ClientWrapper clientWrapper = options.getS3ClientWrapper();
    return clientWrapper.open(path);
  }

  @Override
  public WritableByteChannel create(String spec, String mimeType) throws IOException {
      return null;
  }

  @Override
  public long getSizeBytes(String spec) throws IOException {
    final AmazonS3URI path = new AmazonS3URI(spec);
    S3ClientWrapper clientWrapper = options.getS3ClientWrapper();
    return clientWrapper.fileSize(path);
  }

  @Override
  public boolean isReadSeekEfficient(String spec) throws IOException {
      return false;
  }

  @Override
  public String resolve(String path, String other) throws IOException {
      //return S3Path.fromUri(path).resolve(other).toString();
    return null;
  }
}
