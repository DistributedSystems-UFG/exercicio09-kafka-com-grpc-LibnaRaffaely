"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings

import temperature_pb2 as temperature__pb2

GRPC_GENERATED_VERSION = '1.80.0'
GRPC_VERSION = grpc.__version__
_version_not_supported = False

try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True

if _version_not_supported:
    raise RuntimeError(
        f'The grpc package installed is at version {GRPC_VERSION},'
        + ' but the generated code in temperature_pb2_grpc.py depends on'
        + f' grpcio>={GRPC_GENERATED_VERSION}.'
        + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}'
        + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.'
    )


class TemperatureServiceStub(object):
    """Serviço gRPC de consulta de temperatura
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetLatestReading = channel.unary_unary(
                '/temperature.TemperatureService/GetLatestReading',
                request_serializer=temperature__pb2.QueryRequest.SerializeToString,
                response_deserializer=temperature__pb2.SensorReading.FromString,
                _registered_method=True)
        self.GetLatestAverage = channel.unary_unary(
                '/temperature.TemperatureService/GetLatestAverage',
                request_serializer=temperature__pb2.QueryRequest.SerializeToString,
                response_deserializer=temperature__pb2.TemperatureAverage.FromString,
                _registered_method=True)
        self.GetReadingHistory = channel.unary_stream(
                '/temperature.TemperatureService/GetReadingHistory',
                request_serializer=temperature__pb2.HistoryRequest.SerializeToString,
                response_deserializer=temperature__pb2.SensorReading.FromString,
                _registered_method=True)
        self.GetAverageHistory = channel.unary_stream(
                '/temperature.TemperatureService/GetAverageHistory',
                request_serializer=temperature__pb2.HistoryRequest.SerializeToString,
                response_deserializer=temperature__pb2.TemperatureAverage.FromString,
                _registered_method=True)


class TemperatureServiceServicer(object):
    """Serviço gRPC de consulta de temperatura
    """

    def GetLatestReading(self, request, context):
        """Última leitura bruta registrada
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetLatestAverage(self, request, context):
        """Última média calculada (janela de 2 horas)
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetReadingHistory(self, request, context):
        """Histórico de leituras brutas (server-side streaming)
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetAverageHistory(self, request, context):
        """Histórico de médias calculadas (server-side streaming)
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_TemperatureServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetLatestReading': grpc.unary_unary_rpc_method_handler(
                    servicer.GetLatestReading,
                    request_deserializer=temperature__pb2.QueryRequest.FromString,
                    response_serializer=temperature__pb2.SensorReading.SerializeToString,
            ),
            'GetLatestAverage': grpc.unary_unary_rpc_method_handler(
                    servicer.GetLatestAverage,
                    request_deserializer=temperature__pb2.QueryRequest.FromString,
                    response_serializer=temperature__pb2.TemperatureAverage.SerializeToString,
            ),
            'GetReadingHistory': grpc.unary_stream_rpc_method_handler(
                    servicer.GetReadingHistory,
                    request_deserializer=temperature__pb2.HistoryRequest.FromString,
                    response_serializer=temperature__pb2.SensorReading.SerializeToString,
            ),
            'GetAverageHistory': grpc.unary_stream_rpc_method_handler(
                    servicer.GetAverageHistory,
                    request_deserializer=temperature__pb2.HistoryRequest.FromString,
                    response_serializer=temperature__pb2.TemperatureAverage.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'temperature.TemperatureService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('temperature.TemperatureService', rpc_method_handlers)


class TemperatureService(object):
    """Serviço gRPC de consulta de temperatura
    """

    @staticmethod
    def GetLatestReading(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/temperature.TemperatureService/GetLatestReading',
            temperature__pb2.QueryRequest.SerializeToString,
            temperature__pb2.SensorReading.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetLatestAverage(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/temperature.TemperatureService/GetLatestAverage',
            temperature__pb2.QueryRequest.SerializeToString,
            temperature__pb2.TemperatureAverage.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetReadingHistory(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(
            request,
            target,
            '/temperature.TemperatureService/GetReadingHistory',
            temperature__pb2.HistoryRequest.SerializeToString,
            temperature__pb2.SensorReading.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetAverageHistory(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(
            request,
            target,
            '/temperature.TemperatureService/GetAverageHistory',
            temperature__pb2.HistoryRequest.SerializeToString,
            temperature__pb2.TemperatureAverage.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)
