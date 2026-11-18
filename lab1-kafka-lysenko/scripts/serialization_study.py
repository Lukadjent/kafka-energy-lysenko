"""JSON/Avro/Protobuf round trips and a real local schema-registry/Kafka example."""
from __future__ import annotations
import io
import json
import statistics
import struct
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.request
import urllib.error

from common import ROOT, CONFIG, configure_stdout
import avro.io
import avro.schema
from avro.compatibility import ReaderWriterCompatibilityChecker, SchemaCompatibilityType
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

NUMERIC = ("power_output", "efficiency", "temperature", "voltage", "current", "water_flow", "water_level")


def schema_definition():
    fields = []
    for name in ("device_id", "timestamp", "power_output", "efficiency", "temperature", "voltage", "current", "status", "location", "maintenance_hours", "water_flow", "water_level", "turbine_type"):
        kind = "double" if name in NUMERIC else "long" if name == "maintenance_hours" else "string"
        if name == "location":
            kind = {"type": "record", "name": "Location", "fields": [{"name": "lat", "type": "double"}, {"name": "lon", "type": "double"}]}
        fields.append({"name": name, "type": kind})
    return {"type": "record", "name": "HydroTelemetry", "namespace": "ua.lysenko.lab1", "fields": fields}


class LocalRegistry(BaseHTTPRequestHandler):
    """Educational Avro registry: BACKWARD against the latest subject schema.

    This is a deliberately small local demonstration, not a Confluent service.
    """
    schemas = {}
    subjects = {}

    def log_message(self, *_):
        pass

    def respond(self, code, value):
        body = json.dumps(value).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if not self.path.startswith("/subjects/") or not self.path.endswith("/versions"):
            return self.respond(404, {"error": "Unsupported endpoint"})
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        schema = json.loads(body["schema"])
        subject = self.path.split("/")[2]
        parsed_new = avro.schema.parse(json.dumps(schema))
        if subject in self.subjects:
            parsed_old = avro.schema.parse(self.schemas[self.subjects[subject]])
            result = ReaderWriterCompatibilityChecker().get_compatibility(parsed_new, parsed_old)
            if result.compatibility != SchemaCompatibilityType.compatible:
                return self.respond(409, {"error": "Incompatible Avro schema", "mode": "BACKWARD",
                    "messages": sorted(result.messages)})
        canonical = json.dumps(schema, sort_keys=True)
        identifier = next((identifier for identifier, item in self.schemas.items() if item == canonical), len(self.schemas) + 1)
        self.schemas[identifier] = canonical
        self.subjects[subject] = identifier
        (ROOT / "data" / "registry.json").write_text(json.dumps({"schemas": self.schemas, "subjects": self.subjects}, indent=2), encoding="utf-8")
        self.respond(200, {"id": identifier})

    def do_GET(self):
        if self.path.startswith("/schemas/ids/"):
            identifier = int(self.path.rsplit("/", 1)[1])
            if identifier in self.schemas:
                return self.respond(200, {"schema": self.schemas[identifier]})
        self.respond(404, {"error": "Unknown schema"})


def protobuf_class():
    file = descriptor_pb2.FileDescriptorProto(name="hydro.proto", package="ua.lysenko", syntax="proto3")
    location = file.message_type.add(name="Location")
    for index, name in enumerate(("lat", "lon"), 1):
        location.field.add(name=name, number=index, type=1, label=1)
    telemetry = file.message_type.add(name="HydroTelemetry")
    names = ("device_id", "timestamp", "power_output", "efficiency", "temperature", "voltage", "current", "status", "location", "maintenance_hours", "water_flow", "water_level", "turbine_type")
    for index, name in enumerate(names, 1):
        kind = 1 if name in NUMERIC else 3 if name == "maintenance_hours" else 11 if name == "location" else 9
        field = telemetry.field.add(name=name, number=index, type=kind, label=1)
        if name == "location":
            field.type_name = ".ua.lysenko.Location"
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("ua.lysenko.HydroTelemetry"))


def main():
    configure_stdout()
    evidence = ROOT / "evidence"
    records = [json.loads(line) for line in (ROOT / "data" / "hydro-1000.jsonl").read_text(encoding="utf-8").splitlines()]
    schema = schema_definition()
    (ROOT / "config" / "hydro.avsc").write_text(json.dumps(schema, indent=2), encoding="utf-8")
    parsed = avro.schema.parse(json.dumps(schema))
    writer = avro.io.DatumWriter(parsed)
    reader = avro.io.DatumReader(parsed)
    proto = protobuf_class()
    server = ThreadingHTTPServer(("127.0.0.1", 8081), LocalRegistry)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        request = urllib.request.Request("http://127.0.0.1:8081/subjects/hydro-telemetry-lysenko-value/versions",
            data=json.dumps({"schema": json.dumps(schema)}).encode(), headers={"Content-Type": "application/json"})
        schema_id = json.load(urllib.request.urlopen(request))["id"]
        cold = time.perf_counter_ns()
        fetched = json.load(urllib.request.urlopen(f"http://127.0.0.1:8081/schemas/ids/{schema_id}"))
        assert json.loads(fetched["schema"]) == schema
        registry_lookup_ms = (time.perf_counter_ns() - cold) / 1e6

        # Exercise the schema contract described in lecture 4, not only ID lookup.
        def register(definition):
            request = urllib.request.Request("http://127.0.0.1:8081/subjects/hydro-telemetry-lysenko-value/versions",
                data=json.dumps({"schema": json.dumps(definition)}).encode(), headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request) as response:
                return json.load(response)

        evolved = json.loads(json.dumps(schema))
        evolved["fields"].append({"name": "sensor_quality", "type": ["null", "double"], "default": None})
        evolved_id = register(evolved)["id"]
        assert evolved_id != schema_id
        invalid = json.loads(json.dumps(evolved))
        next(field for field in invalid["fields"] if field["name"] == "power_output")["type"] = "string"
        try:
            register(invalid)
            raise AssertionError("Registry accepted incompatible power_output type")
        except urllib.error.HTTPError as error:
            assert error.code == 409
            rejection = json.load(error)
        compatibility = {"mode": "BACKWARD against latest subject version",
            "optional_field_with_default_accepted_id": evolved_id,
            "incompatible_type_rejected_http": 409, "rejection": rejection}

        def avro_encode(item):
            output = io.BytesIO()
            output.write(b"\x00" + struct.pack(">I", schema_id))
            writer.write(item, avro.io.BinaryEncoder(output))
            return output.getvalue()

        def avro_decode(payload):
            if payload[:5] != b"\x00" + struct.pack(">I", schema_id):
                raise ValueError("Unexpected registry schema id")
            return reader.read(avro.io.BinaryDecoder(io.BytesIO(payload[5:])))

        evolved_reader = avro.io.DatumReader(parsed, avro.schema.parse(json.dumps(evolved)))
        old_payload = avro_encode(records[0])
        evolved_record = evolved_reader.read(avro.io.BinaryDecoder(io.BytesIO(old_payload[5:])))
        assert evolved_record == {**records[0], "sensor_quality": None}
        compatibility["new_reader_reads_old_payload_with_default"] = True
        (ROOT / "config" / "hydro-v2.avsc").write_text(json.dumps(evolved, indent=2), encoding="utf-8")

        def proto_encode(item):
            value = proto()
            for key, item_value in item.items():
                if key == "location":
                    value.location.lat = item_value["lat"]
                    value.location.lon = item_value["lon"]
                else:
                    setattr(value, key, item_value)
            return value.SerializeToString()

        def proto_decode(payload):
            value = proto.FromString(payload)
            return {name: {"lat": value.location.lat, "lon": value.location.lon} if name == "location" else getattr(value, name) for name in records[0]}

        formats = {
            "json": (lambda item: json.dumps(item, separators=(",", ":"), ensure_ascii=False).encode(), lambda payload: json.loads(payload)),
            "avro_registry": (avro_encode, avro_decode),
            "protobuf": (proto_encode, proto_decode),
        }
        results = []
        for name, (encode, decode) in formats.items():
            payloads = [encode(item) for item in records]
            assert all(decode(payload) == item for item, payload in zip(records, payloads))
            timings = []
            for _ in range(5):
                start = time.perf_counter_ns()
                for repeat in range(10):
                    for item in records:
                        encode(item)
                encode_us = (time.perf_counter_ns() - start) / 10000 / 1000
                start = time.perf_counter_ns()
                for repeat in range(10):
                    for payload in payloads:
                        decode(payload)
                decode_us = (time.perf_counter_ns() - start) / 10000 / 1000
                timings.append({"encode_us": encode_us, "decode_us": decode_us})
            results.append({"format": name, "mean_bytes": statistics.mean(map(len, payloads)),
                "min_bytes": min(map(len, payloads)), "max_bytes": max(map(len, payloads)),
                "encode_us_median": statistics.median(item["encode_us"] for item in timings),
                "decode_us_median": statistics.median(item["decode_us"] for item in timings),
                "timings": timings, "round_trip_records": len(records)})
            print(f"SERIALIZATION {name}: {results[-1]['mean_bytes']:.1f} bytes, {results[-1]['encode_us_median']:.2f} us encode", flush=True)

        topic = "hydro-avro-lysenko-" + str(int(time.time()))
        admin = KafkaAdminClient(bootstrap_servers=CONFIG["bootstrap_servers"])
        admin.create_topics([NewTopic(topic, 4, 1)])
        admin.close()
        producer = KafkaProducer(bootstrap_servers=CONFIG["bootstrap_servers"], client_id="avro-producer-lysenko", acks="all")
        for item in records[:10]:
            producer.send(topic, key=item["device_id"].encode(), value=avro_encode(item)).get(timeout=30)
        producer.close()
        consumer = KafkaConsumer(topic, bootstrap_servers=CONFIG["bootstrap_servers"], auto_offset_reset="earliest",
            group_id=topic + "-group", enable_auto_commit=False, consumer_timeout_ms=15000)
        received = []
        for message in consumer:
            received.append(avro_decode(message.value))
            if len(received) == 10:
                break
        consumer.close()
        assert sorted(item["device_id"] for item in received) == sorted(item["device_id"] for item in records[:10])
        summary = {"formats": results, "registry": {"implementation": "educational local REST registry; not Confluent Schema Registry",
            "schema_id": schema_id, "cold_lookup_ms": registry_lookup_ms, "url": "http://127.0.0.1:8081",
            "wire_header_bytes": 5, "kafka_round_trip": len(received), "topic": topic,
            "compatibility": compatibility}}
        (evidence / "serialization.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print("SERIALIZATION_RESULT " + json.dumps(summary), flush=True)
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()

