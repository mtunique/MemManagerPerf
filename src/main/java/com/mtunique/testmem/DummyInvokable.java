package com.mtunique.testmem;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

public class DummyInvokable extends AbstractInvokable {

    @Override
    public void invoke() {}

    @Override
    public ClassLoader getUserCodeClassLoader() {
        return getClass().getClassLoader();
    }

    @Override
    public int getCurrentNumberOfSubtasks() {
        return 1;
    }

    @Override
    public int getIndexInSubtaskGroup() {
        return 0;
    }

    @Override
    public final Configuration getTaskConfiguration() {
        return new Configuration();
    }

    @Override
    public final Configuration getJobConfiguration() {
        return new Configuration();
    }

    @Override
    public ExecutionConfig getExecutionConfig() {
        return new ExecutionConfig();
    }
}
