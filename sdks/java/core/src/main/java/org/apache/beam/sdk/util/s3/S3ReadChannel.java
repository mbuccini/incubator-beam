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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a ReadableByteChannel for Amazon S3.
 */
public final class S3ReadChannel implements ReadableByteChannel {

  private static final Logger LOG = LoggerFactory.getLogger(S3ReadChannel.class);

  private final AmazonS3 s3client;
  private final String bucket;
  private final String key;

  @VisibleForTesting
  ReadableByteChannel readChannel;


  public S3ReadChannel(AmazonS3 s3client, String bucket, String key) {
    this.s3client = s3client;
    this.bucket = bucket;
    this.key = key;
  }

  @Override
  public int read(ByteBuffer buffer) throws IOException {
    if (buffer.hasRemaining()) {
      return 0;
    }

    int totalBytesRead;

    try (S3Object s3object = s3client.getObject(bucket, key)) {
      readChannel = Channels.newChannel(s3object.getObjectContent());
      totalBytesRead = readChannel.read(buffer);
    } catch (AmazonServiceException ase) {
      LOG.error("Service exception while trying to fetch object: {}",
          s3client.getUrl(bucket, key),
          ase);
      throw ase;
    } catch (IOException ioe) {
      LOG.error("Exception while reading from the channel");
      throw ioe;
    } finally {
      readChannel.close();
    }

    return totalBytesRead;
  }

  @Override
  public boolean isOpen() {
    return false;
  }

  @Override
  public void close() throws IOException {
    if (readChannel != null) {
      readChannel.close();
    }
  }
}
