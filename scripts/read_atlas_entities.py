#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys


def parse_properties(path: str):
    props = {}
    if not path or not os.path.exists(path):
        return props
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            props[key.strip()] = value.strip()
    return props


def build_consume_cmd(topic, broker, container, from_beginning):
    offset = "start" if from_beginning else "end"
    return [
        "docker",
        "exec",
        "-i",
        container,
        "rpk",
        "topic",
        "consume",
        topic,
        "--brokers",
        broker,
        "-o",
        offset,
        "--format",
        "%v\\n",
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Read Atlas entity change events from Kafka using rpk.")
    parser.add_argument(
        "--config",
        default="/Users/daniel.henneberger/atlas-metastore/deploy/conf/atlas-application.properties",
        help="Path to atlas-application.properties for defaults.",
    )
    parser.add_argument("--bootstrap", help="Kafka bootstrap servers override.")
    parser.add_argument("--topic", help="Kafka topic override.")
    parser.add_argument("--container", default="redpanda", help="Docker container name running rpk.")
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Start at earliest offset (default is latest).",
    )
    parser.add_argument(
        "--dump-file",
        help="Write all consumed events to a file (JSON lines).",
    )
    parser.add_argument(
        "--dump-only",
        action="store_true",
        help="Do not print to stdout; only write to --dump-file.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON events to stdout.",
    )
    args = parser.parse_args()

    props = parse_properties(args.config)
    broker = args.bootstrap or props.get("atlas.kafka.bootstrap.servers", "localhost:9092")
    topic = args.topic or props.get("atlas.notification.topics", "ATLAS_ENTITIES")

    cmd = build_consume_cmd(topic, broker, args.container, args.from_beginning)

    out_handle = None
    if args.dump_file:
        out_handle = open(args.dump_file, "a", encoding="utf-8")

    print(f"Reading from topic={topic} bootstrap={broker} container={args.container}")
    print("Starting from earliest offset" if args.from_beginning else "Starting from latest offset")
    if args.dump_file:
        print(f"Dumping to {args.dump_file}")

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except FileNotFoundError as exc:
        print(f"Failed to start rpk: {exc}", file=sys.stderr)
        return 2

    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            payload = line.rstrip("\n")
            if out_handle:
                out_handle.write(payload + "\n")
                out_handle.flush()

            if args.dump_only:
                continue

            if args.pretty:
                try:
                    parsed = json.loads(payload)
                    print(json.dumps(parsed, indent=2, sort_keys=True))
                except json.JSONDecodeError:
                    print(payload)
            else:
                print(payload)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        if out_handle:
            out_handle.close()
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    return 0


if __name__ == "__main__":
    sys.exit(main())
