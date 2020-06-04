/*
 *  Copyright (c) 2020 Temporal Technologies, Inc. All Rights Reserved
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.samples.common;

import io.temporal.client.WorkflowClientInterceptorBase;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.proto.common.WorkflowExecution;
import io.temporal.samples.common.MultiClusterWorkflowClient.ManagedClient;
import io.temporal.workflow.Functions;
import java.lang.reflect.Type;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MultiClusterWorkflowClientInterceptor extends WorkflowClientInterceptorBase {

  private ManagedClient client;

  @Override
  public WorkflowStub newUntypedWorkflowStub(
      String workflowType, WorkflowOptions options, WorkflowStub next) {
    return new MultiClusterWorkflowStub(
        client, super.newUntypedWorkflowStub(workflowType, options, next));
  }

  @Override
  public WorkflowStub newUntypedWorkflowStub(
      WorkflowExecution execution, Optional<String> workflowType, WorkflowStub next) {
    return new MultiClusterWorkflowStub(
        client, super.newUntypedWorkflowStub(execution, workflowType, next));
  }

  void setManagedClient(ManagedClient client) {
    this.client = client;
  }

  static class MultiClusterWorkflowStub implements WorkflowStub {
    private final ManagedClient client;
    private final WorkflowStub next;

    private MultiClusterWorkflowStub(ManagedClient client, WorkflowStub next) {
      this.client = client;
      this.next = next;
    }

    private void withRecordError(Functions.Proc f) {
      withRecordError(
          () -> {
            f.apply();
            return null;
          });
    }

    private <T> T withRecordError(Functions.Func<T> f) {
      try {
        return f.apply();
      } catch (RuntimeException e) {
        recordError(e);
        throw e;
      }
    }

    private void recordError(Throwable t) {
      if (t != null) {
        client.recordError(t);
      }
    }

    @Override
    public void signal(String signalName, Object... args) {
      withRecordError(() -> next.signal(signalName, args));
    }

    @Override
    public WorkflowExecution start(Object... args) {
      return withRecordError(() -> next.start(args));
    }

    @Override
    public WorkflowExecution signalWithStart(
        String signalName, Object[] signalArgs, Object[] startArgs) {
      return withRecordError(() -> next.signalWithStart(signalName, signalArgs, startArgs));
    }

    @Override
    public Optional<String> getWorkflowType() {
      return withRecordError(next::getWorkflowType);
    }

    @Override
    public WorkflowExecution getExecution() {
      return withRecordError(next::getExecution);
    }

    @Override
    public <R> R getResult(Class<R> resultClass, Type resultType) {
      return withRecordError(() -> next.getResult(resultClass, resultType));
    }

    @Override
    public <R> CompletableFuture<R> getResultAsync(Class<R> resultClass, Type resultType) {
      return next.getResultAsync(resultClass, resultType).whenComplete((r, t) -> recordError(t));
    }

    @Override
    public <R> R getResult(Class<R> resultClass) {
      return withRecordError(() -> next.getResult(resultClass));
    }

    @Override
    public <R> CompletableFuture<R> getResultAsync(Class<R> resultClass) {
      return next.getResultAsync(resultClass).whenComplete((r, t) -> recordError(t));
    }

    @Override
    public <R> R getResult(long timeout, TimeUnit unit, Class<R> resultClass, Type resultType)
        throws TimeoutException {
      try {
        return withRecordError(
            () -> {
              try {
                return next.getResult(timeout, unit, resultClass, resultType);
              } catch (TimeoutException e) {
                throw new WrappedAsRuntimeException(e);
              }
            });
      } catch (WrappedAsRuntimeException e) {
        throw e.getCause();
      }
    }

    @Override
    public <R> R getResult(long timeout, TimeUnit unit, Class<R> resultClass)
        throws TimeoutException {
      try {
        return withRecordError(
            () -> {
              try {
                return next.getResult(timeout, unit, resultClass);
              } catch (TimeoutException e) {
                throw new WrappedAsRuntimeException(e);
              }
            });
      } catch (WrappedAsRuntimeException e) {
        throw e.getCause();
      }
    }

    @Override
    public <R> CompletableFuture<R> getResultAsync(
        long timeout, TimeUnit unit, Class<R> resultClass, Type resultType) {
      return next.getResultAsync(timeout, unit, resultClass, resultType)
          .whenComplete((r, t) -> recordError(t));
    }

    @Override
    public <R> CompletableFuture<R> getResultAsync(
        long timeout, TimeUnit unit, Class<R> resultClass) {
      return next.getResultAsync(timeout, unit, resultClass).whenComplete((r, t) -> recordError(t));
    }

    @Override
    public <R> R query(String queryType, Class<R> resultClass, Object... args) {
      return withRecordError(() -> next.query(queryType, resultClass, args));
    }

    @Override
    public <R> R query(String queryType, Class<R> resultClass, Type resultType, Object... args) {
      return withRecordError(() -> next.query(queryType, resultClass, resultType, args));
    }

    @Override
    public void cancel() {
      withRecordError(next::cancel);
    }

    @Override
    public Optional<WorkflowOptions> getOptions() {
      return withRecordError(next::getOptions);
    }
  }

  private static class WrappedAsRuntimeException extends RuntimeException {
    private WrappedAsRuntimeException(TimeoutException cause) {
      super(cause);
    }

    @Override
    public synchronized TimeoutException getCause() {
      return (TimeoutException) super.getCause();
    }
  }
}
