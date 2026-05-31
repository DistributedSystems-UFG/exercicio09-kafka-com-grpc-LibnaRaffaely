
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    6,
    31,
    1,
    '',
    'temperature.proto'
)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x11temperature.proto\x12\x0btemperature\"J\n\rSensorReading\x12\x11\n\tsensor_id\x18\x01 \x01(\t\x12\x13\n\x0btemperature\x18\x02 \x01(\x01\x12\x11\n\ttimestamp\x18\x03 \x01(\x03\"x\n\x12TemperatureAverage\x12\x11\n\tsensor_id\x18\x01 \x01(\t\x12\x0f\n\x07\x61verage\x18\x02 \x01(\x01\x12\x14\n\x0cwindow_start\x18\x03 \x01(\x03\x12\x12\n\nwindow_end\x18\x04 \x01(\x03\x12\x14\n\x0csample_count\x18\x05 \x01(\x05\"!\n\x0cQueryRequest\x12\x11\n\tsensor_id\x18\x01 \x01(\t\"Q\n\x0eHistoryRequest\x12\x11\n\tsensor_id\x18\x01 \x01(\t\x12\x16\n\x0e\x66rom_timestamp\x18\x02 \x01(\x03\x12\x14\n\x0cto_timestamp\x18\x03 \x01(\x03\x32\xd4\x02\n\x12TemperatureService\x12I\n\x10GetLatestReading\x12\x19.temperature.QueryRequest\x1a\x1a.temperature.SensorReading\x12N\n\x10GetLatestAverage\x12\x19.temperature.QueryRequest\x1a\x1f.temperature.TemperatureAverage\x12N\n\x11GetReadingHistory\x12\x1b.temperature.HistoryRequest\x1a\x1a.temperature.SensorReading0\x01\x12S\n\x11GetAverageHistory\x12\x1b.temperature.HistoryRequest\x1a\x1f.temperature.TemperatureAverage0\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'temperature_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_SENSORREADING']._serialized_start=34
  _globals['_SENSORREADING']._serialized_end=108
  _globals['_TEMPERATUREAVERAGE']._serialized_start=110
  _globals['_TEMPERATUREAVERAGE']._serialized_end=230
  _globals['_QUERYREQUEST']._serialized_start=232
  _globals['_QUERYREQUEST']._serialized_end=265
  _globals['_HISTORYREQUEST']._serialized_start=267
  _globals['_HISTORYREQUEST']._serialized_end=348
  _globals['_TEMPERATURESERVICE']._serialized_start=351
  _globals['_TEMPERATURESERVICE']._serialized_end=691