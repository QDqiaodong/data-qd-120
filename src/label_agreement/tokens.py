"""Strict streaming JSON tokenizer for bounded files.

The tokenizer reads RFC 8259 JSON one token at a time from a buffered binary
stream, so inputs of multiple GiB never have to be resident in memory.  Its
acceptance rules match the strict standard-library decoding used by
:func:`label_agreement.common.read`:

* only structural whitespace, the six literals/structural tokens and strict
  grammar strings/numbers are accepted, so ``NaN``/``Infinity`` and malformed
  numbers fail exactly as they do through ``json.loads``;
* individual string and number tokens are decoded with ``json.loads`` (the
  tokens themselves are bounded), preserving escape, UTF-8 and surrogate
  semantics;
* a UTF-8 BOM and any trailing byte after the top value are rejected.

Duplicate object keys are enforced by the layered parser (see
:mod:`label_agreement.streaming`), which keeps a key set per object.
"""
import hashlib
import json
import re

NUMBER_RE = re.compile(
    rb'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z')
WHITESPACE = b' \t\r\n'
MAX_TOKEN_BYTES = 1 << 20  # individual tokens are short; defense in depth


class StreamingJSONError(ValueError):
    """Raised when a streamed document violates the strict JSON grammar."""


class Tokenizer:
    """Pull-based tokenizer over a binary file-like object."""

    def __init__(self, stream, max_bytes, hasher=None):
        self._stream = stream
        self._max_bytes = max_bytes
        self._hasher = hasher or hashlib.sha256()
        self._buf = b''
        self._pos = 0
        self._total = 0
        self._consumed = 0  # absolute byte offset of the current read position
        self._eof = False
        self._peeked = None
        self._started = False
        self._depth = 0
        self._root_open = False
        self._root_close_offset = None

    @property
    def digest(self):
        return self._hasher.copy()

    def hexdigest(self):
        return self._hasher.hexdigest()

    def _fill(self):
        if self._pos:
            self._consumed += self._pos
            self._buf = self._buf[self._pos:]
            self._pos = 0
        if self._eof:
            return False
        chunk = self._stream.read(1 << 16)
        if not chunk:
            self._eof = True
            return False
        self._total += len(chunk)
        if self._total > self._max_bytes:
            raise StreamingJSONError('JSON document exceeds the byte limit')
        self._hasher.update(chunk)
        self._buf += chunk
        return True

    def _absolute_position(self):
        return self._consumed + self._pos

    def _unconsumed_tail(self):
        """Bytes from the current read position to the document end."""
        return self._buf[self._pos:]

    def _need_byte(self):
        if self._pos >= len(self._buf):
            if not self._fill():
                raise StreamingJSONError('unexpected end of JSON document')
        return self._buf[self._pos]

    def skip_whitespace(self):
        while True:
            tail = self._buf[self._pos:].lstrip(WHITESPACE)
            self._pos = len(self._buf) - len(tail)
            if self._pos < len(self._buf) or self._eof or not self._fill():
                return

    def peek(self):
        if self._peeked is None:
            self._peeked = self.next_token()
        return self._peeked

    def next_token(self):
        if self._peeked is not None:
            token = self._peeked
            self._peeked = None
            return token
        self.skip_whitespace()
        if not self._started:
            self._started = True
            if self._buf[:3] == b'\xef\xbb\xbf':
                raise StreamingJSONError('UTF-8 BOM is not allowed')
        if self._pos >= len(self._buf):
            if self._eof:
                return ('eof', None)
            if not self._fill():
                return ('eof', None)
        byte = self._buf[self._pos]
        if byte in (ord('{'), ord('}'), ord('['), ord(']'),
                    ord(':'), ord(',')):
            if byte in (ord('{'), ord('[')):
                if self._depth == 0:
                    self._root_open = True
                self._depth += 1
            elif byte in (ord('}'), ord(']')):
                self._depth -= 1
                if self._root_open and self._depth == 0:
                    self._root_close_offset = self._absolute_position()
            token = (chr(byte), None)
            self._pos += 1
            return token
        if byte == ord('"'):
            return ('str', self._read_string())
        if byte in b'-0123456789':
            return ('num', self._read_number())
        for literal in (b'true', b'false', b'null'):
            if self._buf_startswith(literal):
                self._pos += len(literal)
                return ('lit', literal.decode('ascii'))
        # NaN, Infinity, bare words and any other byte are invalid JSON here.
        raise StreamingJSONError('invalid JSON token at byte %d' %
                                 (self._total - len(self._buf) + self._pos))

    def _buf_startswith(self, literal):
        while len(self._buf) - self._pos < len(literal) and not self._eof:
            if not self._fill():
                break
        return self._buf[self._pos:self._pos + len(literal)] == literal

    def _read_string(self):
        self._pos += 1  # opening quote
        parts = []
        total = 2
        escaped = False
        while True:
            buf = self._buf
            quote = buf.find(b'"', self._pos)
            slash = buf.find(b'\\', self._pos)
            if quote == -1 and slash == -1:
                segment = bytes(buf[self._pos:])
                if segment and min(segment) < 0x20:
                    raise StreamingJSONError('unescaped control character in string')
                parts.append(segment)
                total += len(segment)
                self._pos = len(buf)
                if not self._fill():
                    raise StreamingJSONError('unterminated JSON string')
                continue
            if slash == -1 or (quote != -1 and quote < slash):
                segment = bytes(buf[self._pos:quote])
                if segment and min(segment) < 0x20:
                    raise StreamingJSONError('unescaped control character in string')
                parts.append(segment)
                total += len(segment) + 1
                self._pos = quote + 1
                if total > MAX_TOKEN_BYTES:
                    raise StreamingJSONError('JSON string token exceeds the token size bound')
                if not escaped:
                    # Common case: an escape-free UTF-8 string.  Decode the raw
                    # bytes directly instead of round-tripping through json.
                    try:
                        return b''.join(parts).decode('utf-8')
                    except UnicodeDecodeError as error:
                        raise StreamingJSONError(str(error))
                raw = b'"' + b''.join(parts) + b'"'
                try:
                    return json.loads(raw)
                except ValueError as error:
                    raise StreamingJSONError(str(error))
            # Escape sequence: copy the plain prefix, then consume the escape.
            escaped = True
            segment = bytes(buf[self._pos:slash])
            if segment and min(segment) < 0x20:
                raise StreamingJSONError('unescaped control character in string')
            parts.append(segment)
            total += len(segment)
            self._pos = slash
            self._consume_escape(parts)
            total += 2

    def _consume_escape(self, parts):
        self._pos += 1  # backslash
        marker = self._need_byte()
        if marker == ord('u'):
            self._pos += 1
            digits = bytearray()
            for _ in range(4):
                digit = self._need_byte()
                if digit not in b'0123456789abcdefABCDEF':
                    raise StreamingJSONError('invalid \\u escape in string')
                digits.append(digit)
                self._pos += 1
            parts.append(b'\\u' + bytes(digits))
            return
        if marker not in b'"\\/bfnrt':
            raise StreamingJSONError('invalid escape in string')
        parts.append(b'\\' + bytes([marker]))
        self._pos += 1

    def _read_number(self):
        parts = []
        while True:
            buf = self._buf
            probe = self._pos
            while probe < len(buf) and buf[probe] in b'0123456789+-eE.':
                probe += 1
            parts.append(bytes(buf[self._pos:probe]))
            self._pos = probe
            if probe < len(buf) or self._eof:
                break
            self._fill()
        raw = b''.join(parts)
        if not NUMBER_RE.fullmatch(raw):
            raise StreamingJSONError('invalid JSON number')
        if len(raw) > 64:
            raise StreamingJSONError('JSON number token exceeds the token size bound')
        try:
            value = json.loads(raw)
        except ValueError as error:
            raise StreamingJSONError(str(error))
        if isinstance(value, float) and not value == value:
            raise StreamingJSONError('nonfinite JSON number')
        return value

    def expect(self, kind):
        token, value = self.next_token()
        if token != kind:
            raise StreamingJSONError('expected %s token, got %s' % (kind, token))
        return value

    def expect_end(self, allow_trailing_lf=False):
        # Reject any trailing byte, whitespace included: a JSON document is
        # exactly one value.  Reports published by this package carry one
        # trailing LF (matching common.save's encoding), which callers may
        # opt into.  Remaining chunks are drained so the byte limit still
        # applies to oversized inputs whose first value parsed successfully.
        if self._root_close_offset is None:
            raise StreamingJSONError('document did not contain a complete root value')
        # Drain remaining chunks without consuming whitespace: the raw
        # unconsumed tail is the complete trailing-data evidence.
        while not self._eof:
            self._fill()
        tail = self._unconsumed_tail()
        allowed = b'\n' if allow_trailing_lf else b''
        if tail != allowed:
            raise StreamingJSONError('trailing data after JSON document')

    def reject_bom(self):
        """Reject a leading UTF-8 BOM before the first token is consumed."""
        while self._pos >= len(self._buf) and not self._eof:
            self._fill()
        if self._buf[:3] == b'\xef\xbb\xbf':
            raise StreamingJSONError('UTF-8 BOM is not allowed')
