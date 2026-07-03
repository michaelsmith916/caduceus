#!/usr/bin/env python3
import argparse
import ipaddress
import socket
import struct
import time
from collections import defaultdict

SERVICE = "_caduceus._tcp.local"
MDNS_ADDR = ("224.0.0.251", 5353)

TYPE_A = 1
TYPE_PTR = 12
TYPE_TXT = 16
TYPE_AAAA = 28
TYPE_SRV = 33


def dns_name(name):
    return b"".join(bytes([len(part)]) + part.encode() for part in name.split(".")) + b"\0"


def build_query(service):
    return (
        b"\x00\x00"  # transaction id
        b"\x00\x00"  # flags
        b"\x00\x01"  # questions
        b"\x00\x00"  # answers
        b"\x00\x00"  # authority
        b"\x00\x00"  # additional
        + dns_name(service)
        + b"\x00\x0c"  # PTR
        + b"\x00\x01"  # IN
    )


def read_name(data, offset):
    labels = []
    jumped = False
    end = offset
    seen = set()

    while True:
        if offset >= len(data):
            raise ValueError("name overruns packet")
        length = data[offset]
        if length & 0xC0 == 0xC0:
            if offset + 1 >= len(data):
                raise ValueError("compressed name pointer overruns packet")
            pointer = ((length & 0x3F) << 8) | data[offset + 1]
            if pointer in seen:
                raise ValueError("compressed name pointer loop")
            seen.add(pointer)
            if not jumped:
                end = offset + 2
            offset = pointer
            jumped = True
            continue
        if length == 0:
            if not jumped:
                end = offset + 1
            break
        offset += 1
        if offset + length > len(data):
            raise ValueError("label overruns packet")
        labels.append(data[offset : offset + length].decode("utf-8", errors="replace"))
        offset += length

    return ".".join(labels), end


def read_records(data):
    if len(data) < 12:
        return []
    qdcount, ancount, nscount, arcount = struct.unpack("!HHHH", data[4:12])
    offset = 12

    for _ in range(qdcount):
        _, offset = read_name(data, offset)
        offset += 4

    records = []
    for _ in range(ancount + nscount + arcount):
        name, offset = read_name(data, offset)
        if offset + 10 > len(data):
            raise ValueError("record header overruns packet")
        rtype, rclass, ttl, rdlength = struct.unpack("!HHIH", data[offset : offset + 10])
        offset += 10
        rdata_offset = offset
        rdata = data[offset : offset + rdlength]
        offset += rdlength
        records.append((name, rtype, rclass, ttl, rdata_offset, rdata))
    return records


def parse_txt(rdata):
    out = []
    offset = 0
    while offset < len(rdata):
        length = rdata[offset]
        offset += 1
        value = rdata[offset : offset + length]
        offset += length
        out.append(value.decode("utf-8", errors="replace"))
    return out


def parse_multiaddr(value):
    parts = value.split("/")
    parsed = {"raw": value}
    for index, part in enumerate(parts):
        if part in {"ip4", "ip6", "tcp", "udp", "p2p"} and index + 1 < len(parts):
            parsed[part] = parts[index + 1]
    return parsed


def useful_addr(parsed):
    ip_value = parsed.get("ip4") or parsed.get("ip6")
    if not ip_value:
        return True, "non-ip"
    try:
        ip = ipaddress.ip_address(ip_value)
    except ValueError:
        return False, "invalid-ip"
    if ip.is_loopback:
        return False, "loopback"
    if ip.is_unspecified:
        return False, "unspecified"
    if ip.is_link_local:
        return False, "link-local"
    if ip.is_multicast:
        return False, "multicast"
    if ip_value == "10.255.255.254":
        return False, "virtual-adapter"
    if ip.version == 4 and ip.packed[0] == 172 and 17 <= ip.packed[1] <= 19:
        return False, "docker-bridge"
    return True, "usable"


def describe_record(data, record):
    name, rtype, _, _, rdata_offset, rdata = record
    if rtype == TYPE_PTR:
        target, _ = read_name(data, rdata_offset)
        return f"PTR {name} -> {target}", []
    if rtype == TYPE_SRV and len(rdata) >= 6:
        priority, weight, port = struct.unpack("!HHH", rdata[:6])
        target, _ = read_name(data, rdata_offset + 6)
        return f"SRV {name} port={port} target={target} priority={priority} weight={weight}", []
    if rtype == TYPE_TXT:
        values = parse_txt(rdata)
        return f"TXT {name}", values
    if rtype == TYPE_A and len(rdata) == 4:
        return f"A {name} -> {socket.inet_ntop(socket.AF_INET, rdata)}", []
    if rtype == TYPE_AAAA and len(rdata) == 16:
        return f"AAAA {name} -> {socket.inet_ntop(socket.AF_INET6, rdata)}", []
    return f"TYPE{rtype} {name} ({len(rdata)} bytes)", []


def main():
    parser = argparse.ArgumentParser(description="Probe and decode Caduceus mDNS responses.")
    parser.add_argument("--service", default=SERVICE)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--expect-ip", action="append", default=[], help="IP address expected in dnsaddr TXT records.")
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.settimeout(1)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

    print(f"Querying {args.service} via mDNS...")
    sock.sendto(build_query(args.service), MDNS_ADDR)

    deadline = time.time() + args.timeout
    seen = False
    peers = defaultdict(dict)
    ignored = defaultdict(list)
    expected = {ipaddress.ip_address(value) for value in args.expect_ip}
    found_ips = set()

    while time.time() < deadline:
        try:
            data, addr = sock.recvfrom(8192)
        except socket.timeout:
            continue

        if args.service.encode() not in data and b"dnsaddr=" not in data:
            continue

        seen = True
        print(f"\nResponse from {addr[0]}:{addr[1]}")
        try:
            records = read_records(data)
        except ValueError as exc:
            print(f"  Could not parse DNS packet: {exc}")
            continue

        for record in records:
            summary, txt_values = describe_record(data, record)
            print(f"  {summary}")
            for value in txt_values:
                print(f"    {value}")
                if not value.startswith("dnsaddr="):
                    continue
                raw = value.removeprefix("dnsaddr=")
                parsed = parse_multiaddr(raw)
                peer = parsed.get("p2p", "unknown-peer")
                usable, reason = useful_addr(parsed)
                if usable:
                    peers[peer][raw] = parsed
                    ip_value = parsed.get("ip4") or parsed.get("ip6")
                    if ip_value:
                        found_ips.add(ipaddress.ip_address(ip_value))
                else:
                    ignored[peer].append((raw, reason))

    if not seen:
        print(f"No {args.service} mDNS responses seen.")
        return

    print("\nUsable libp2p dnsaddr records:")
    if not peers:
        print("  none")
    for peer, addrs in sorted(peers.items()):
        print(f"  {peer}")
        for raw in sorted(addrs):
            print(f"    {raw}")

    if ignored:
        print("\nIgnored dnsaddr records:")
        for peer, values in sorted(ignored.items()):
            print(f"  {peer}")
            for raw, reason in sorted(values):
                print(f"    {raw} ({reason})")

    if expected:
        print("\nExpected IP check:")
        for ip in sorted(expected, key=str):
            result = "found" if ip in found_ips else "missing"
            print(f"  {ip}: {result}")


if __name__ == "__main__":
    main()
