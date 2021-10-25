package io.opentelemetry.instrumentation.grpc.v1_6;

import static cloud.filibuster.instrumentation.Helper.getDisableInstrumentationFromEnvironment;
import static cloud.filibuster.instrumentation.Helper.getDisableServerCommunicationFromEnvironment;

import cloud.filibuster.instrumentation.datatypes.VectorClock;
import cloud.filibuster.instrumentation.instrumentors.FilibusterClientInstrumentor;
import cloud.filibuster.instrumentation.storage.ContextStorage;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONObject;

public class OpenTelemetryFilibusterClientInterceptor implements ClientInterceptor {
    private static final Logger logger = Logger.getLogger(OpenTelemetryFilibusterClientInterceptor.class.getName());

    @SuppressWarnings("FieldCanBeFinal")
    protected String serviceName;

    @SuppressWarnings("FieldCanBeFinal")
    protected ContextStorage contextStorage;

    public static Boolean disableServerCommunication = false;
    public static Boolean disableInstrumentation = false;

    private final Instrumenter<GrpcRequest, Status> clientInstrumentor;

    @SuppressWarnings("FieldCanBeLocal")
    private final ContextPropagators propagators;

    private static boolean shouldInstrument() {
        if (disableInstrumentation) {
            return false;
        }

        return !getDisableInstrumentationFromEnvironment();
    }

    private static boolean shouldCommunicateWithServer() {
        if (disableServerCommunication) {
            return false;
        }

        return !getDisableServerCommunicationFromEnvironment();
    }

    private static Status generateCorrectStatusForAbort(FilibusterClientInstrumentor filibusterClientInstrumentor) {
        JSONObject forcedException = filibusterClientInstrumentor.getForcedException();
        JSONObject forcedExceptionMetadata = forcedException.getJSONObject("metadata");
        String codeStr = forcedExceptionMetadata.getString("code");
        Status.Code code = Status.Code.valueOf(codeStr);
        Status status = Status.fromCode(code);
        return status;
    }

    private static void generateAndThrowException(FilibusterClientInstrumentor filibusterClientInstrumentor) {
        JSONObject forcedException = filibusterClientInstrumentor.getForcedException();

        // Create the exception to throw.
        String exceptionNameString = forcedException.getString("name");
        JSONObject forcedExceptionMetadata = forcedException.getJSONObject("metadata");
        String causeString = forcedExceptionMetadata.getString("cause");
        String codeStr = forcedExceptionMetadata.getString("code");
        Status.Code code = Status.Code.valueOf(codeStr);
        StatusRuntimeException status = Status.fromCode(code).asRuntimeException();

        // Notify Filibuster of failure.
        HashMap<String, String> additionalMetadata = new HashMap<>();
        additionalMetadata.put("code", codeStr);
        filibusterClientInstrumentor.afterInvocationWithException(exceptionNameString, causeString, additionalMetadata);

        // Throw the runtime exception.
        throw status;
    }

    private static void generateAndThrowExceptionFromFailureMetadata(FilibusterClientInstrumentor filibusterClientInstrumentor) {
        JSONObject failureMetadata = filibusterClientInstrumentor.getFailureMetadata();
        JSONObject exception = failureMetadata.getJSONObject("exception");
        JSONObject exceptionMetadata = exception.getJSONObject("metadata");

        // Create the exception to throw.
        String exceptionNameString = "io.grpc.StatusRuntimeException";
        String codeStr = exceptionMetadata.getString("code");
        Status.Code code = Status.Code.valueOf(codeStr);
        StatusRuntimeException status = Status.fromCode(code).asRuntimeException();
        String causeString = "";

        // Notify Filibuster of failure.
        HashMap<String, String> additionalMetadata = new HashMap<>();
        additionalMetadata.put("name", exceptionNameString);
        additionalMetadata.put("code", codeStr);
        filibusterClientInstrumentor.afterInvocationWithException(exceptionNameString, causeString, additionalMetadata);

        // Throw the runtime exception.
        throw status;
    }

    public OpenTelemetryFilibusterClientInterceptor(Instrumenter<GrpcRequest, Status> clientInstrumentor, ContextPropagators propagators) {
        this.serviceName = System.getenv("SERVICE_NAME");
        this.contextStorage = new OpenTelemetryContextStorage();
        this.clientInstrumentor = clientInstrumentor;
        this.propagators = propagators;
    }

    public OpenTelemetryFilibusterClientInterceptor(String serviceName, Instrumenter<GrpcRequest, Status> clientInstrumentor, ContextPropagators propagators) {
        this.serviceName = serviceName;
        this.contextStorage = new OpenTelemetryContextStorage();
        this.clientInstrumentor = clientInstrumentor;
        this.propagators = propagators;
    }

    @Override
    public <REQUEST, RESPONSE> ClientCall<REQUEST, RESPONSE> interceptCall(
            MethodDescriptor<REQUEST, RESPONSE> method, CallOptions callOptions, Channel next) {
        GrpcRequest request = new GrpcRequest(method, null, null);
        Context parentContext = Context.current();

        Context context;

        if (clientInstrumentor != null) {
            context = clientInstrumentor.start(parentContext, request);
        } else {
            context = Context.current();
        }

        // call other interceptors first.
        final ClientCall<REQUEST, RESPONSE> result = next.newCall(method, callOptions);

        // return the filibuster client interceptor.
        return new FilibusterClientCall<>(result, parentContext, context, request, serviceName, contextStorage, method);
    }

    // *********************************************************************
    // Client caller.

    final class FilibusterClientCall<REQUEST, RESPONSE>
            extends ForwardingClientCall.SimpleForwardingClientCall<REQUEST, RESPONSE> {

        final private String serviceName;
        final private MethodDescriptor<REQUEST, RESPONSE> method;
        final private ContextStorage contextStorage;
        private final Context parentContext;
        private final Context context;
        private final GrpcRequest request;

        FilibusterClientCall(ClientCall<REQUEST, RESPONSE> delegate,
                             Context parentContext,
                             Context context,
                             GrpcRequest request,
                             String serviceName,
                             ContextStorage contextStorage,
                             MethodDescriptor<REQUEST, RESPONSE> method) {
            super(delegate);
            this.serviceName = serviceName;
            this.method = method;
            this.contextStorage = contextStorage;
            this.parentContext = parentContext;
            this.context = context;
            this.request = request;
        }

        // ******************************************************************************************
        // Accessors for metadata.
        // ******************************************************************************************

        public String getRequestIdFromMetadata() {
            return contextStorage.getRequestId();
        }

        public VectorClock getVectorClockFromMetadata() {
            return contextStorage.getVectorClock();
        }

        public VectorClock getOriginVectorClockFromMetadata() {
            return contextStorage.getOriginVectorClock();
        }

        public String getExecutionIndexFromMetadata() {
            return contextStorage.getExecutionIndex();
        }

        // ******************************************************************************************
        // Implementation.
        // ******************************************************************************************

        @Override
        public void start(Listener<RESPONSE> responseListener, Metadata headers) {
            logger.log(Level.INFO, "INSIDE: start!");

            // ******************************************************************************************
            // Extract callsite information.
            // ******************************************************************************************

            String grpcFullMethodName = method.getFullMethodName();
            String grpcRpcName = grpcFullMethodName.substring(grpcFullMethodName.indexOf("/") + 1);
            String grpcServiceName = grpcFullMethodName.substring(0, grpcFullMethodName.indexOf("/"));

            logger.log(Level.INFO, "grpcFullMethodName: " + grpcFullMethodName);
            logger.log(Level.INFO, "grpcServiceName: " + grpcServiceName);
            logger.log(Level.INFO, "grpcRpcName: " + grpcRpcName);

            // ******************************************************************************************
            // Figure out if we are inside of instrumentation.
            // ******************************************************************************************

            String instrumentationRequestStr = headers.get(
                    Metadata.Key.of("x-filibuster-instrumentation", Metadata.ASCII_STRING_MARSHALLER));
            logger.log(Level.INFO, "instrumentationRequestStr: " + instrumentationRequestStr);
            boolean instrumentationRequest = Boolean.parseBoolean(instrumentationRequestStr);
            logger.log(Level.INFO, "instrumentationRequest: " + instrumentationRequest);

            if (! shouldInstrument() || instrumentationRequest) {
                try (Scope ignored = context.makeCurrent()) {
                    super.start(responseListener, headers);
                } catch (Throwable e) {
                    if (clientInstrumentor != null) {
                        clientInstrumentor.end(context, request, null, e);
                    }

                    throw e;
                }
            } else {
                // ******************************************************************************************
                // Prepare for invocation.
                // ******************************************************************************************

                FilibusterClientInstrumentor filibusterClientInstrumentor = new FilibusterClientInstrumentor(
                        serviceName,
                        shouldCommunicateWithServer(),
                        contextStorage
                );
                filibusterClientInstrumentor.prepareForInvocation();

                // ******************************************************************************************
                // Record invocation.
                // ******************************************************************************************

                filibusterClientInstrumentor.beforeInvocation(grpcServiceName, grpcFullMethodName, "");

                JSONObject forcedException = filibusterClientInstrumentor.getForcedException();
                JSONObject failureMetadata = filibusterClientInstrumentor.getFailureMetadata();

                logger.log(Level.INFO, "forcedException: " + forcedException);
                logger.log(Level.INFO, "failureMetadata: " + failureMetadata);

                // ******************************************************************************************
                // Attach metadata to outgoing request.
                // ******************************************************************************************

                logger.log(Level.INFO, "requestId: " + filibusterClientInstrumentor.getRequestId());

                if (filibusterClientInstrumentor.getRequestId() != null) {
                    headers.put(
                            Metadata.Key.of("x-filibuster-request-id", Metadata.ASCII_STRING_MARSHALLER),
                            filibusterClientInstrumentor.getRequestId()
                    );
                }

                if (filibusterClientInstrumentor.getGeneratedId() > -1) {
                    headers.put(
                            Metadata.Key.of("x-filibuster-generated-id", Metadata.ASCII_STRING_MARSHALLER),
                            String.valueOf(filibusterClientInstrumentor.getGeneratedId())
                    );
                }

                headers.put(
                        Metadata.Key.of("x-filibuster-vclock", Metadata.ASCII_STRING_MARSHALLER),
                        filibusterClientInstrumentor.getVectorClock().toString()
                );
                headers.put(
                        Metadata.Key.of("x-filibuster-origin-vclock", Metadata.ASCII_STRING_MARSHALLER),
                        filibusterClientInstrumentor.getOriginVectorClock().toString()
                );
                headers.put(
                        Metadata.Key.of("x-filibuster-execution-index", Metadata.ASCII_STRING_MARSHALLER),
                        filibusterClientInstrumentor.getExecutionIndex().toString()
                );

                if (forcedException != null) {
                    JSONObject forcedExceptionMetadata = forcedException.getJSONObject("metadata");

                    if (forcedExceptionMetadata.has("sleep")) {
                        int sleepInterval = forcedExceptionMetadata.getInt("sleep");
                        headers.put(
                                Metadata.Key.of("x-filibuster-forced-sleep", Metadata.ASCII_STRING_MARSHALLER),
                                String.valueOf(sleepInterval)
                        );
                    } else {
                        headers.put(
                                Metadata.Key.of("x-filibuster-forced-sleep", Metadata.ASCII_STRING_MARSHALLER),
                                String.valueOf(0)
                        );
                    }
                }

                // ******************************************************************************************
                // If we need to override the response, do it now before proceeding.
                // ******************************************************************************************

                if (failureMetadata != null && filibusterClientInstrumentor.shouldAbort()) {
                    generateAndThrowExceptionFromFailureMetadata(filibusterClientInstrumentor);
                }

                // ******************************************************************************************
                // If we need to throw, this is where we throw.
                // ******************************************************************************************

                if (forcedException != null && filibusterClientInstrumentor.shouldAbort()) {
                    generateAndThrowException(filibusterClientInstrumentor);
                }

                // ******************************************************************************************
                // Issue request.
                // ******************************************************************************************

                try (Scope ignored = context.makeCurrent()) {
                    super.start(new FilibusterClientCallListener<>(
                            responseListener, parentContext, context, request, filibusterClientInstrumentor), headers);
                } catch (Throwable e) {
                    if (clientInstrumentor != null) {
                        clientInstrumentor.end(context, request, null, e);
                    }

                    throw e;
                }
            }
        }

        // This method is invoked with the message from the Client to the Service.
        // message: type of the message issued from the Client (e.g., Hello$HelloRequest)
        @Override
        public void sendMessage(REQUEST message) {
            logger.log(Level.INFO, "INSIDE: sendMessage!");
            logger.log(Level.INFO, "message: " + message.toString());
            try (Scope ignored = context.makeCurrent()) {
                super.sendMessage(message);
            } catch (Throwable e) {
                if (clientInstrumentor != null) {
                    clientInstrumentor.end(context, request, null, e);
                }

                throw e;
            }
        }
    }

    // *********************************************************************
    // Client caller listener.

    @SuppressWarnings("ClassCanBeStatic")
    final class FilibusterClientCallListener<RESPONSE>
            extends ForwardingClientCallListener.SimpleForwardingClientCallListener<RESPONSE> {

        private final FilibusterClientInstrumentor filibusterClientInstrumentor;
        private final Context parentContext;
        private final Context context;
        private final GrpcRequest request;

        FilibusterClientCallListener(ClientCall.Listener<RESPONSE> delegate,
                                     Context parentContext,
                                     Context context,
                                     GrpcRequest request,
                                     FilibusterClientInstrumentor filibusterClientInstrumentor) {
            super(delegate);
            this.filibusterClientInstrumentor = filibusterClientInstrumentor;
            this.parentContext = parentContext;
            this.context = context;
            this.request = request;
        }

        // invoked on successful response with the message from the Server to the Client
        // message: type of message issued from the Server to the Client (e.g., Hello$HelloReply)
        @Override
        public void onMessage(RESPONSE message) {
            logger.log(Level.INFO, "INSIDE: onMessage!");
            logger.log(Level.INFO, "message: " + message);

            if (! filibusterClientInstrumentor.shouldAbort()) {
                // Request completed normally, but we want to throw the exception anyway, generate and throw.
                generateAndThrowException(filibusterClientInstrumentor);
            } else {
                // Request completed normally.

                // Notify Filibuster of complete invocation with the proper response.
                String className = message.getClass().getName();
                HashMap<String, String> returnValueProperties = new HashMap<>();
                filibusterClientInstrumentor.afterInvocationComplete(className, returnValueProperties);

                // Delegate.
                try (Scope ignored = context.makeCurrent()) {
                    delegate().onMessage(message);
                }
            }
        }

        // invoked on an error: status set to a status message
        // Status.code = FAILED_PRECONDITION, description = ..., cause = ...
        // trailers metadata headers.
        @Override
        public void onClose(Status status, Metadata trailers) {
            if (clientInstrumentor != null) {
                clientInstrumentor.end(context, request, status, status.getCause());
            }

            logger.log(Level.INFO, "INSIDE: onClose!");
            logger.log(Level.INFO, "status: " + status);
            logger.log(Level.INFO, "trailers: " + trailers);

            if (! filibusterClientInstrumentor.shouldAbort()) {
                Status rewrittenStatus = generateCorrectStatusForAbort(filibusterClientInstrumentor);

                try (Scope ignored = parentContext.makeCurrent()) {
                    delegate().onClose(rewrittenStatus, trailers);
                }
            }

            if (! status.isOk()) {
                // Request completed -- if it completed with a failure, it will be coming here for
                // the first time (didn't call onMessage) and therefore, we need to notify the Filibuster
                // server that the call completed with failure.  If it completed successfully, we would
                // have already notified the Filibuster server in the onMessage callback.

                // Notify Filibuster of error.
                HashMap<String, String> additionalMetadata = new HashMap<>();
                additionalMetadata.put("code", status.getCode().toString());
                String exceptionName = "io.grpc.StatusRuntimeException";
                // exception cause is always null, because it doesn't serialize and pass through even if provided.
                filibusterClientInstrumentor.afterInvocationWithException(exceptionName, null, additionalMetadata);
            }

            try (Scope ignored = parentContext.makeCurrent()) {
                delegate().onClose(status, trailers);
            }
        }

        @Override
        public void onReady() {
            logger.log(Level.INFO, "INSIDE: onReady!");
            try (Scope ignored = context.makeCurrent()) {
                delegate().onReady();
            }
        }
    }
}
