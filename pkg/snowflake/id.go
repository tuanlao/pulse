package snowflake

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strconv"
)

// ID is a snowflake id. The encoding helpers (String/Base2/.../JSON/Bytes) are
// layout-independent and live here; the layout-dependent extractors (Time, Node,
// Step) live on *Generator because they need the generator's epoch and bit shifts
// (this package keeps no process-global layout, unlike bwmarrin/snowflake).
type ID int64

// Sentinel errors for the textual decoders.
var (
	ErrInvalidBase32 = errors.New("snowflake: invalid base32 id")
	ErrInvalidBase58 = errors.New("snowflake: invalid base58 id")
	errInvalidJSON   = errors.New("snowflake: invalid JSON id (expected a quoted integer)")
)

// Base32/Base58 alphabets match bwmarrin/snowflake so encodings interoperate with
// that ecosystem.
const (
	encodeBase32Map = "ybndrfg8ejkmcpqxot1uwisza345h769"
	encodeBase58Map = "123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ"
)

var (
	decodeBase32Map [256]byte
	decodeBase58Map [256]byte
)

func init() {
	for i := range decodeBase32Map {
		decodeBase32Map[i] = 0xFF
	}
	for i := 0; i < len(encodeBase32Map); i++ {
		decodeBase32Map[encodeBase32Map[i]] = byte(i)
	}
	for i := range decodeBase58Map {
		decodeBase58Map[i] = 0xFF
	}
	for i := 0; i < len(encodeBase58Map); i++ {
		decodeBase58Map[encodeBase58Map[i]] = byte(i)
	}
}

// Int64 returns the id as an int64.
func (f ID) Int64() int64 { return int64(f) }

// ParseInt64 wraps an int64 as an ID.
func ParseInt64(id int64) ID { return ID(id) }

// String returns the base-10 representation.
func (f ID) String() string { return strconv.FormatInt(int64(f), 10) }

// ParseString parses a base-10 id.
func ParseString(id string) (ID, error) {
	i, err := strconv.ParseInt(id, 10, 64)
	return ID(i), err
}

// Base2 returns the base-2 representation.
func (f ID) Base2() string { return strconv.FormatInt(int64(f), 2) }

// ParseBase2 parses a base-2 id.
func ParseBase2(id string) (ID, error) {
	i, err := strconv.ParseInt(id, 2, 64)
	return ID(i), err
}

// Base36 returns the base-36 representation.
func (f ID) Base36() string { return strconv.FormatInt(int64(f), 36) }

// ParseBase36 parses a base-36 id.
func ParseBase36(id string) (ID, error) {
	i, err := strconv.ParseInt(id, 36, 64)
	return ID(i), err
}

// Base32 returns a base-32 representation using the package alphabet.
func (f ID) Base32() string {
	if f < 32 {
		return string(encodeBase32Map[f])
	}
	b := make([]byte, 0, 12)
	for f >= 32 {
		b = append(b, encodeBase32Map[f%32])
		f /= 32
	}
	b = append(b, encodeBase32Map[f])
	reverse(b)
	return string(b)
}

// ParseBase32 parses a base-32 id produced by Base32.
func ParseBase32(b []byte) (ID, error) {
	var id int64
	for i := range b {
		if decodeBase32Map[b[i]] == 0xFF {
			return -1, ErrInvalidBase32
		}
		id = id*32 + int64(decodeBase32Map[b[i]])
	}
	return ID(id), nil
}

// Base58 returns a base-58 representation using the package alphabet.
func (f ID) Base58() string {
	if f < 58 {
		return string(encodeBase58Map[f])
	}
	b := make([]byte, 0, 11)
	for f >= 58 {
		b = append(b, encodeBase58Map[f%58])
		f /= 58
	}
	b = append(b, encodeBase58Map[f])
	reverse(b)
	return string(b)
}

// ParseBase58 parses a base-58 id produced by Base58.
func ParseBase58(b []byte) (ID, error) {
	var id int64
	for i := range b {
		if decodeBase58Map[b[i]] == 0xFF {
			return -1, ErrInvalidBase58
		}
		id = id*58 + int64(decodeBase58Map[b[i]])
	}
	return ID(id), nil
}

// Base64 returns the standard base-64 encoding of Bytes (the decimal string).
func (f ID) Base64() string { return base64.StdEncoding.EncodeToString(f.Bytes()) }

// ParseBase64 parses a base-64 id produced by Base64.
func ParseBase64(id string) (ID, error) {
	b, err := base64.StdEncoding.DecodeString(id)
	if err != nil {
		return -1, err
	}
	return ParseBytes(b)
}

// Bytes returns the base-10 representation as a byte slice.
func (f ID) Bytes() []byte { return []byte(f.String()) }

// ParseBytes parses a base-10 id from its byte-slice form.
func ParseBytes(id []byte) (ID, error) {
	i, err := strconv.ParseInt(string(id), 10, 64)
	return ID(i), err
}

// IntBytes returns the id as 8 big-endian bytes.
func (f ID) IntBytes() [8]byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(f))
	return b
}

// ParseIntBytes parses an id from its 8 big-endian bytes.
func ParseIntBytes(id [8]byte) ID {
	return ID(int64(binary.BigEndian.Uint64(id[:])))
}

// MarshalJSON encodes the id as a JSON string so it survives JavaScript's 53-bit
// number truncation.
func (f ID) MarshalJSON() ([]byte, error) {
	buf := make([]byte, 0, 22)
	buf = append(buf, '"')
	buf = strconv.AppendInt(buf, int64(f), 10)
	buf = append(buf, '"')
	return buf, nil
}

// UnmarshalJSON decodes an id from a JSON string (or, leniently, the unquoted
// integer form some encoders emit is rejected to keep the contract explicit).
func (f *ID) UnmarshalJSON(b []byte) error {
	if len(b) < 3 || b[0] != '"' || b[len(b)-1] != '"' {
		return errInvalidJSON
	}
	i, err := strconv.ParseInt(string(b[1:len(b)-1]), 10, 64)
	if err != nil {
		return err
	}
	*f = ID(i)
	return nil
}

// MarshalText encodes the id as its base-10 text (for map keys, URL params, ...).
func (f ID) MarshalText() ([]byte, error) { return f.Bytes(), nil }

// UnmarshalText decodes the id from its base-10 text.
func (f *ID) UnmarshalText(b []byte) error {
	i, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*f = ID(i)
	return nil
}

func reverse(b []byte) {
	for x, y := 0, len(b)-1; x < y; x, y = x+1, y-1 {
		b[x], b[y] = b[y], b[x]
	}
}
