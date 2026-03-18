"""Deterministic unit tests for base64 value encoding (T016)."""

from packed_kv_cosmos.packing import decode_value, encode_value


class TestValueEncoding:
    """Round-trip and edge-case tests for opaque byte <-> base64 encoding."""

    def test_round_trip_simple(self) -> None:
        original = b"hello"
        assert decode_value(encode_value(original)) == original

    def test_round_trip_empty(self) -> None:
        original = b""
        assert decode_value(encode_value(original)) == original

    def test_round_trip_binary(self) -> None:
        original = bytes(range(256))
        assert decode_value(encode_value(original)) == original

    def test_round_trip_single_byte(self) -> None:
        assert decode_value(encode_value(b"\x00")) == b"\x00"
        assert decode_value(encode_value(b"\xff")) == b"\xff"

    def test_encode_produces_ascii(self) -> None:
        encoded = encode_value(b"\x01\x02\x03")
        assert encoded.isascii()

    def test_encode_produces_base64(self) -> None:
        """Verify the encoding matches standard base64."""
        import base64

        value = b"cosmos packed kv"
        assert encode_value(value) == base64.b64encode(value).decode("ascii")

    def test_deterministic(self) -> None:
        val = b"same bytes"
        assert encode_value(val) == encode_value(val)
