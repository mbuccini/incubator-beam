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

package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.TestStream.ElementEvent;
import org.apache.beam.sdk.testing.TestStream.Event;
import org.apache.beam.sdk.testing.TestStream.EventType;
import org.apache.beam.sdk.testing.TestStream.ProcessingTimeEvent;
import org.apache.beam.sdk.testing.TestStream.WatermarkEvent;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** The {@link TransformEvaluatorFactory} for the {@link TestStream} primitive. */
class TestStreamEvaluatorFactory implements TransformEvaluatorFactory {
  private final EvaluationContext evaluationContext;

  TestStreamEvaluatorFactory(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
  }

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) {
    return createEvaluator((AppliedPTransform) application);
  }

  @Override
  public void cleanup() throws Exception {}

  /**
   * Returns the evaluator for the provided application of {@link TestStream}, or null if it is
   * already in use.
   *
   * <p>The documented behavior of {@link TestStream} requires the output of one event to travel
   * completely through the pipeline before any additional event, so additional instances that have
   * a separate collection of events cannot be created.
   */
  private <InputT, OutputT> TransformEvaluator<? super InputT> createEvaluator(
      AppliedPTransform<PBegin, PCollection<OutputT>, TestStream<OutputT>> application) {
    return (TransformEvaluator<InputT>) new Evaluator<>(application, evaluationContext);
  }

  private static class Evaluator<T> implements TransformEvaluator<TestStreamIndex<T>> {
    private final AppliedPTransform<PBegin, PCollection<T>, TestStream<T>> application;
    private final EvaluationContext context;
    private final StepTransformResult.Builder resultBuilder;

    private Evaluator(
        AppliedPTransform<PBegin, PCollection<T>, TestStream<T>> application,
        EvaluationContext context) {
      this.application = application;
      this.context = context;
      this.resultBuilder = StepTransformResult.withoutHold(application);
    }

    @Override
    public void processElement(WindowedValue<TestStreamIndex<T>> element) throws Exception {
      TestStreamIndex<T> streamIndex = element.getValue();
      List<Event<T>> events = streamIndex.getTestStream().getEvents();
      int index = streamIndex.getIndex();
      Instant watermark = element.getTimestamp();
      Event<T> event = events.get(index);

      if (event.getType().equals(EventType.ELEMENT)) {
        UncommittedBundle<T> bundle = context.createBundle(application.getOutput());
        for (TimestampedValue<T> elem : ((ElementEvent<T>) event).getElements()) {
          bundle.add(
              WindowedValue.timestampedValueInGlobalWindow(elem.getValue(), elem.getTimestamp()));
        }
        resultBuilder.addOutput(bundle);
      }

      if (event.getType().equals(EventType.WATERMARK)) {
        watermark = ((WatermarkEvent<T>) event).getWatermark();
      }

      if (event.getType().equals(EventType.PROCESSING_TIME)) {
        ((TestClock) context.getClock())
            .advance(((ProcessingTimeEvent<T>) event).getProcessingTimeAdvance());
      }

      TestStreamIndex<T> next = streamIndex.next();
      if (next.getIndex() < events.size()) {
        resultBuilder.addUnprocessedElements(
            Collections.singleton(WindowedValue.timestampedValueInGlobalWindow(next, watermark)));
      }
    }

    @Override
    public TransformResult finishBundle() throws Exception {
      return resultBuilder.build();
    }
  }

  @VisibleForTesting
  static class TestClock implements Clock {
    private final AtomicReference<Instant> currentTime =
        new AtomicReference<>(BoundedWindow.TIMESTAMP_MIN_VALUE);

    public void advance(Duration amount) {
      Instant now = currentTime.get();
      currentTime.compareAndSet(now, now.plus(amount));
    }

    @Override
    public Instant now() {
      return currentTime.get();
    }
  }

  private static class TestClockSupplier implements Supplier<Clock> {
    @Override
    public Clock get() {
      return new TestClock();
    }
  }

  static class DirectTestStreamFactory implements PTransformOverrideFactory {
    @Override
    public <InputT extends PInput, OutputT extends POutput> PTransform<InputT, OutputT> override(
        PTransform<InputT, OutputT> transform) {
      if (transform instanceof TestStream) {
        return (PTransform<InputT, OutputT>)
            new DirectTestStream<OutputT>((TestStream<OutputT>) transform);
      }
      return transform;
    }

    private static class DirectTestStream<T> extends PTransform<PBegin, PCollection<T>> {
      private final TestStream<T> original;

      private DirectTestStream(TestStream<T> transform) {
        this.original = transform;
      }

      @Override
      public PCollection<T> apply(PBegin input) {
        PipelineRunner<?> runner = input.getPipeline().getRunner();
        checkState(
            runner instanceof DirectRunner,
            "%s can only be used when running with the %s",
            getClass().getSimpleName(),
            DirectRunner.class.getSimpleName());
        ((DirectRunner) runner).setClockSupplier(new TestClockSupplier());
        return PCollection.<T>createPrimitiveOutputInternal(
                input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED)
            .setCoder(original.getValueCoder());
      }
    }
  }

  static class InputProvider implements RootInputProvider {
    private final EvaluationContext evaluationContext;

    InputProvider(EvaluationContext evaluationContext) {
      this.evaluationContext = evaluationContext;
    }

    @Override
    public Collection<CommittedBundle<?>> getInitialInputs(
        AppliedPTransform<?, ?, ?> transform, int targetParallelism) {
      return createInputBundle((AppliedPTransform) transform);
    }

    private <T> Collection<CommittedBundle<?>> createInputBundle(
        AppliedPTransform<PBegin, ?, TestStream<T>> transform) {
      CommittedBundle<TestStreamIndex<T>> initialBundle =
          evaluationContext
              .<TestStreamIndex<T>>createRootBundle()
              .add(WindowedValue.valueInGlobalWindow(TestStreamIndex.of(transform.getTransform())))
              .commit(BoundedWindow.TIMESTAMP_MAX_VALUE);
      return Collections.<CommittedBundle<?>>singleton(initialBundle);
    }
  }

  @AutoValue
  abstract static class TestStreamIndex<T> {
    static <T> TestStreamIndex<T> of(TestStream<T> stream) {
      return new AutoValue_TestStreamEvaluatorFactory_TestStreamIndex<>(stream, 0);
    }

    abstract TestStream<T> getTestStream();
    abstract int getIndex();

    TestStreamIndex<T> next() {
      return new AutoValue_TestStreamEvaluatorFactory_TestStreamIndex<>(
          getTestStream(), getIndex() + 1);
    }
  }
}
