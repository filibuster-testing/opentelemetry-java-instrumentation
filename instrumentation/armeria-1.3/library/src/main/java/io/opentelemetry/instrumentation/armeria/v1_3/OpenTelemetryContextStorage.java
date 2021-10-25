package io.opentelemetry.instrumentation.armeria.v1_3;

import cloud.filibuster.instrumentation.datatypes.VectorClock;
import cloud.filibuster.instrumentation.storage.ContextStorage;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

public class OpenTelemetryContextStorage implements ContextStorage {
    private static final Logger logger = Logger.getLogger(OpenTelemetryContextStorage.class.getName());

    private Context context;

    // TODO: Context.current() should maybe be cached, who knows?

    public OpenTelemetryContextStorage() {
        this.context = Context.current();
    }

    public Context getContext() {
        return this.context;
    }

    final private static ContextKey<String> vClockKey = ContextKey.named("filibuster-vclock");
    final private static ContextKey<String> originVclockKey = ContextKey.named("filibuster-origin-vclock");
    final private static ContextKey<String> requestIdKey = ContextKey.named("filibuster-request-id");
    final private static ContextKey<String> executionIndexKey = ContextKey.named("filibuster-execution-index");

    @Override
    @Nullable
    public String getRequestId() {
        return Context.current().get(requestIdKey);
    }

    @Override
    @Nullable
    public VectorClock getVectorClock() {
        String vectorClockStr = Context.current().get(vClockKey);

        VectorClock newVclock = new VectorClock();

        if (vectorClockStr != null) {
            newVclock.fromString(vectorClockStr);
        }

        return newVclock;
    }

    @Override
    @Nullable
    public VectorClock getOriginVectorClock() {
        String originVectorClockStr = Context.current().get(originVclockKey);

        VectorClock newVclock = new VectorClock();

        if (originVectorClockStr != null) {
            newVclock.fromString(originVectorClockStr);
        }

        return newVclock;
    }

    @Override
    @Nullable
    public String getExecutionIndex() {
        return Context.current().get(executionIndexKey);
    }

    @Override
    public void setRequestId(String requestId) {
        this.context = this.context.with(requestIdKey, requestId);
        logger.log(Level.SEVERE, "setRequestId: " + requestId);
    }

    @Override
    public void setVectorClock(VectorClock vectorClock) {
        this.context = this.context.with(vClockKey, vectorClock.toString());
        logger.log(Level.SEVERE, "setVectorClock: " + vectorClock);
    }

    @Override
    public void setOriginVectorClock(VectorClock originVectorClock) {
        this.context = this.context.with(originVclockKey, originVectorClock.toString());
        logger.log(Level.SEVERE, "setOriginVectorClock: " + originVectorClock);
    }

    @Override
    public void setExecutionIndex(String executionIndex) {
        this.context = this.context.with(executionIndexKey, executionIndex);
        logger.log(Level.SEVERE, "setExecutionIndex: " + executionIndex);
    }
}
