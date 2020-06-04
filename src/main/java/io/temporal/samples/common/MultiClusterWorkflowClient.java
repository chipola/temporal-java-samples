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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.temporal.client.ActivityCompletionClient;
import io.temporal.client.BatchRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientInterceptor;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.proto.common.WorkflowExecution;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiClusterWorkflowClient implements WorkflowClient {

  private static final Logger LOG = LoggerFactory.getLogger(MultiClusterWorkflowClient.class);

  private static final long MAX_FAILURES = 5;

  private final ManagedClient[] clients;

  private volatile int currentClient = 0;

  private MultiClusterWorkflowClient(
      String[] targets,
      WorkflowServiceStubsOptions serviceOptions,
      WorkflowClientOptions clientOptions) {
    int i = 0;
    clients = new ManagedClient[targets.length];
    for (String target : targets) {
      WorkflowServiceStubsOptions options =
          WorkflowServiceStubsOptions.newBuilder(serviceOptions).setTarget(target).build();
      WorkflowServiceStubs service = WorkflowServiceStubs.newInstance(options);
      if (clientOptions == null) {
        clientOptions = WorkflowClientOptions.getDefaultInstance();
      }
      List<WorkflowClientInterceptor> clientInterceptors = new ArrayList<>();
      if (clientOptions.getInterceptors() != null) {
        clientInterceptors.addAll(Arrays.asList(clientOptions.getInterceptors()));
      }
      MultiClusterWorkflowClientInterceptor interceptor =
          new MultiClusterWorkflowClientInterceptor();
      clientInterceptors.add(interceptor);
      WorkflowClientOptions enhancedClientOptions =
          WorkflowClientOptions.newBuilder(clientOptions)
              .setInterceptors(clientInterceptors.toArray(new WorkflowClientInterceptor[0]))
              .build();
      ManagedClient client =
          new ManagedClient(WorkflowClient.newInstance(service, enhancedClientOptions), target);
      interceptor.setManagedClient(client);
      clients[i++] = client;
    }
  }

  public static WorkflowClient newInstance(String[] targets) {
    return new MultiClusterWorkflowClient(
        targets,
        WorkflowServiceStubsOptions.getDefaultInstance(),
        WorkflowClientOptions.getDefaultInstance());
  }

  public static WorkflowClient newInstance(
      String[] targets, WorkflowServiceStubsOptions serviceOptions) {
    return new MultiClusterWorkflowClient(
        targets, serviceOptions, WorkflowClientOptions.getDefaultInstance());
  }

  public static WorkflowClient newInstance(
      String[] targets,
      WorkflowServiceStubsOptions serviceOptions,
      WorkflowClientOptions clientOptions) {
    return new MultiClusterWorkflowClient(targets, serviceOptions, clientOptions);
  }

  private WorkflowClient client() {
    return clients[currentClient].get();
  }

  @Override
  public WorkflowClientOptions getOptions() {
    return client().getOptions();
  }

  @Override
  public <T> T newWorkflowStub(Class<T> workflowInterface, WorkflowOptions options) {
    return client().newWorkflowStub(workflowInterface, options);
  }

  @Override
  public <T> T newWorkflowStub(Class<T> workflowInterface, String workflowId) {
    return client().newWorkflowStub(workflowInterface, workflowId);
  }

  @Override
  public <T> T newWorkflowStub(
      Class<T> workflowInterface, String workflowId, Optional<String> runId) {
    return client().newWorkflowStub(workflowInterface, workflowId, runId);
  }

  @Override
  public WorkflowStub newUntypedWorkflowStub(String workflowType, WorkflowOptions options) {
    return client().newUntypedWorkflowStub(workflowType, options);
  }

  @Override
  public WorkflowStub newUntypedWorkflowStub(
      String workflowId, Optional<String> runId, Optional<String> workflowType) {
    return client().newUntypedWorkflowStub(workflowId, runId, workflowType);
  }

  @Override
  public WorkflowStub newUntypedWorkflowStub(
      WorkflowExecution execution, Optional<String> workflowType) {
    return client().newUntypedWorkflowStub(execution, workflowType);
  }

  @Override
  public ActivityCompletionClient newActivityCompletionClient() {
    return client().newActivityCompletionClient();
  }

  @Override
  public BatchRequest newSignalWithStartRequest() {
    return client().newSignalWithStartRequest();
  }

  @Override
  public WorkflowExecution signalWithStart(BatchRequest signalWithStartBatch) {
    return client().signalWithStart(signalWithStartBatch);
  }

  @Override
  public WorkflowServiceStubs getWorkflowServiceStubs() {
    return client().getWorkflowServiceStubs();
  }

  class ManagedClient {

    private final WorkflowClient client;
    private final String target;

    private AtomicLong failureCount = new AtomicLong(0);

    private ManagedClient(WorkflowClient client, String target) {
      this.client = client;
      this.target = target;
    }

    private WorkflowClient get() {
      return client;
    }

    void recordError(Throwable t) {
      if (isCriticalFailure(t) && failureCount.incrementAndGet() >= MAX_FAILURES) {
        nextClient(t);
      }
    }

    private boolean isCriticalFailure(Throwable t) {
      return (t instanceof StatusRuntimeException
              && (((StatusRuntimeException) t).getStatus().getCode()
                  == Status.FAILED_PRECONDITION.getCode()))
          || (t instanceof IOException);
    }

    private void nextClient(Throwable t) {
      synchronized (clients) {
        ManagedClient current = clients[currentClient];
        if (current == this) {
          LOG.warn("Marking client down due to error: {}", this, t);
          int nextClient = (currentClient + 1) % clients.length;
          clients[nextClient].failureCount.set(0);
          currentClient = nextClient;
        }
      }
    }

    @Override
    public String toString() {
      return "ManagedClient{"
          + "client="
          + client
          + ", target='"
          + target
          + '\''
          + ", failureCount="
          + failureCount
          + '}';
    }
  }
}
