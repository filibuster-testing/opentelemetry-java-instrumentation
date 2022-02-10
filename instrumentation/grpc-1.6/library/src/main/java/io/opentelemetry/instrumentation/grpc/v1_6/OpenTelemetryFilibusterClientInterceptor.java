package io.opentelemetry.instrumentation.grpc.v1_6;

import cloud.filibuster.instrumentation.datatypes.Callsite;
import cloud.filibuster.instrumentation.instrumentors.FilibusterClientInstrumentor;
import cloud.filibuster.instrumentation.libraries.grpc.NoopClientCall;
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
import org.json.JSONObject;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static cloud.filibuster.instrumentation.Helper.getDisableInstrumentationFromEnvironment;
import static cloud.filibuster.instrumentation.Helper.getDisableServerCommunicationFromEnvironment;

public class OpenTelemetryFilibusterClientInterceptor implements ClientInterceptor {
    private static final Logger logger = Logger.getLogger(OpenTelemetryFilibusterClientInterceptor.class.getName());

    @SuppressWarnings("FieldCanBeFinal")
    protected String serviceName;

    @SuppressWarnings("FieldCanBeFinal")
    protected ContextStorage contextStorage;

    public static Boolean disableServerCommunication = false;
    public static Boolean disableInstrumentation = false;

    @Nullable
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

    private static Status generateException(FilibusterClientInstrumentor filibusterClientInstrumentor) {
        JSONObject forcedException = filibusterClientInstrumentor.getForcedException();

        // Create the exception to throw.
        String exceptionNameString = forcedException.getString("name");
        JSONObject forcedExceptionMetadata = forcedException.getJSONObject("metadata");
        String causeString = forcedExceptionMetadata.getString("cause");
        String codeStr = forcedExceptionMetadata.getString("code");
        Status.Code code = Status.Code.valueOf(codeStr);

        // Notify Filibuster of failure.
        HashMap<String, String> additionalMetadata = new HashMap<>();
        additionalMetadata.put("code", codeStr);
        filibusterClientInstrumentor.afterInvocationWithException(exceptionNameString, causeString, additionalMetadata);

        // Return status.
        return Status.fromCode(code);
    }

    private static Status generateExceptionFromFailureMetadata(FilibusterClientInstrumentor filibusterClientInstrumentor) {
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

        // Return status.
        return Status.fromCode(code);
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

        if (method.getType() != MethodDescriptor.MethodType.UNARY) {
            return next.newCall(method, callOptions);
        }

        GrpcRequest request = new GrpcRequest(method, null, null);
        Context parentContext = Context.current();

        Context context;

        if (clientInstrumentor != null) {
            context = clientInstrumentor.start(parentContext, request);
        } else {
            context = Context.current();
        }

        // return the filibuster client interceptor.
        return new ForwardingClientCall<REQUEST, RESPONSE>() {
            @Nullable
            private ClientCall<REQUEST, RESPONSE> delegate;
            private Listener<RESPONSE> responseListener;

            @Nullable
            private Metadata headers;
            private int requestTokens;
            private FilibusterClientInstrumentor filibusterClientInstrumentor;

            @Override
            protected ClientCall<REQUEST, RESPONSE> delegate() {
                if (delegate == null) {
                    throw new UnsupportedOperationException();
                }
                return delegate;
            }

            @Override
            public void start(Listener<RESPONSE> responseListener, Metadata headers) {
                logger.log(Level.INFO, "INSIDE: start!");

                this.headers = headers;
                this.responseListener = responseListener;
            }

            @Override
            public void request(int requests) {
                if (delegate == null) {
                    requestTokens += requests;
                    return;
                }
                super.request(requests);
            }

            // This method is invoked with the message from the Client to the Service.
            // message: type of the message issued from the Client (e.g., Hello$HelloRequest)
            @Override
            public void sendMessage(REQUEST message) {
                logger.log(Level.INFO, "INSIDE: sendMessage!");
                logger.log(Level.INFO, "message: " + message.toString());

                try (Scope ignored = context.makeCurrent()) {
                    // ******************************************************************************************
                    // Figure out if we are inside of instrumentation.
                    // ******************************************************************************************

                    String instrumentationRequestStr = headers.get(
                            Metadata.Key.of("x-filibuster-instrumentation", Metadata.ASCII_STRING_MARSHALLER));
                    logger.log(Level.INFO, "instrumentationRequestStr: " + instrumentationRequestStr);
                    boolean instrumentationRequest = Boolean.parseBoolean(instrumentationRequestStr);
                    logger.log(Level.INFO, "instrumentationRequest: " + instrumentationRequest);

                    if (! shouldInstrument() || instrumentationRequest) {
                        delegate = next.newCall(method, callOptions);
                        super.start(responseListener, headers);
                        headers = null;
                        if (requestTokens > 0) {
                            super.request(requestTokens);
                            requestTokens = 0;
                        }
                    } else {
                        // ******************************************************************************************
                        // Extract callsite information.
                        // ******************************************************************************************

                        String grpcFullMethodName = method.getFullMethodName();
                        String grpcServiceName = grpcFullMethodName.substring(0, grpcFullMethodName.indexOf("/"));
                        String grpcRpcName = grpcFullMethodName.replace(grpcServiceName + "/", "");

//                        logger.log(Level.INFO, "method: " + method);
                        logger.log(Level.INFO, "grpcFullMethodName: " + grpcFullMethodName);
                        logger.log(Level.INFO, "grpcServiceName: " + grpcServiceName);
                        logger.log(Level.INFO, "grpcRpcName: " + grpcRpcName);

                        // ******************************************************************************************
                        // Construct preliminary call site information.
                        // ******************************************************************************************

                        Callsite callsite = new Callsite(
                                serviceName,
                                grpcServiceName,
                                grpcFullMethodName,
                                message.toString()
                        );

                        // ******************************************************************************************
                        // Prepare for invocation.
                        // ******************************************************************************************

                        this.filibusterClientInstrumentor = new FilibusterClientInstrumentor(
                                serviceName,
                                shouldCommunicateWithServer(),
                                contextStorage,
                                callsite
                        );
                        filibusterClientInstrumentor.prepareForInvocation();

                        // ******************************************************************************************
                        // Record invocation.
                        // ******************************************************************************************

                        filibusterClientInstrumentor.beforeInvocation();

                        // ******************************************************************************************
                        // Attach metadata to outgoing request.
                        // ******************************************************************************************

                        logger.log(Level.INFO, "requestId: " + filibusterClientInstrumentor.getOutgoingRequestId());

                        if (filibusterClientInstrumentor.getOutgoingRequestId() != null) {
                            headers.put(
                                    Metadata.Key.of("x-filibuster-request-id", Metadata.ASCII_STRING_MARSHALLER),
                                    filibusterClientInstrumentor.getOutgoingRequestId()
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

                        // ******************************************************************************************
                        // Get failure information.
                        // ******************************************************************************************

                        JSONObject forcedException = filibusterClientInstrumentor.getForcedException();
                        JSONObject failureMetadata = filibusterClientInstrumentor.getFailureMetadata();

                        logger.log(Level.INFO, "forcedException: " + forcedException);
                        logger.log(Level.INFO, "failureMetadata: " + failureMetadata);

                        // ******************************************************************************************
                        // Setup additional failure headers, if necessary.
                        // ******************************************************************************************

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
                            delegate = new NoopClientCall<REQUEST, RESPONSE>();
                            Status status = generateExceptionFromFailureMetadata(filibusterClientInstrumentor);
                            responseListener.onClose(status, new Metadata());
                            return;
                        }

                        // ******************************************************************************************
                        // If we need to throw, this is where we throw.
                        // ******************************************************************************************

                        if (forcedException != null && filibusterClientInstrumentor.shouldAbort()) {
                            delegate = new NoopClientCall<REQUEST, RESPONSE>();
                            Status status = generateException(filibusterClientInstrumentor);
                            responseListener.onClose(status, new Metadata());
                            return;
                        }

                        delegate = next.newCall(method, callOptions);
                        super.start(new FilibusterClientCallListener<>(
                                responseListener, parentContext, context, request, filibusterClientInstrumentor), headers);
                        headers = null;
                        if (requestTokens > 0) {
                            super.request(requestTokens);
                            requestTokens = 0;
                        }
                    }

                    super.sendMessage(message);
                } catch (Throwable e) {
                    if (clientInstrumentor != null) {
                        clientInstrumentor.end(context, request, null, e);
                    }

                    throw e;
                }
            }
        };
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
                generateException(filibusterClientInstrumentor);
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
