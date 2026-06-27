package snowflake

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestIDConversionsRoundTrip(t *testing.T) {
	ids := []ID{0, 1, 31, 32, 57, 58, 1234567890, 1234567890123456789}
	for _, id := range ids {
		if got, err := ParseString(id.String()); err != nil || got != id {
			t.Fatalf("ParseString(%q)=%d,%v want %d", id.String(), got, err, id)
		}
		if got, err := ParseBase2(id.Base2()); err != nil || got != id {
			t.Fatalf("base2 round-trip %d: got %d, %v", id, got, err)
		}
		if got, err := ParseBase32([]byte(id.Base32())); err != nil || got != id {
			t.Fatalf("base32 round-trip %d: got %d, %v", id, got, err)
		}
		if got, err := ParseBase36(id.Base36()); err != nil || got != id {
			t.Fatalf("base36 round-trip %d: got %d, %v", id, got, err)
		}
		if got, err := ParseBase58([]byte(id.Base58())); err != nil || got != id {
			t.Fatalf("base58 round-trip %d: got %d, %v", id, got, err)
		}
		if got, err := ParseBase64(id.Base64()); err != nil || got != id {
			t.Fatalf("base64 round-trip %d: got %d, %v", id, got, err)
		}
		if got, err := ParseBytes(id.Bytes()); err != nil || got != id {
			t.Fatalf("bytes round-trip %d: got %d, %v", id, got, err)
		}
		if got := ParseIntBytes(id.IntBytes()); got != id {
			t.Fatalf("intbytes round-trip %d: got %d", id, got)
		}
		if got := ParseInt64(id.Int64()); got != id {
			t.Fatalf("int64 round-trip %d: got %d", id, got)
		}
	}
}

func TestIDJSON(t *testing.T) {
	id := ID(987654321012345678)
	b, err := json.Marshal(id)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `"987654321012345678"` {
		t.Fatalf("MarshalJSON = %s, want a quoted integer", b)
	}
	var got ID
	if err := json.Unmarshal(b, &got); err != nil || got != id {
		t.Fatalf("unmarshal round-trip: got %d, %v", got, err)
	}

	// An unquoted number is rejected (the contract is a quoted integer).
	if err := got.UnmarshalJSON([]byte("123")); !errors.Is(err, errInvalidJSON) {
		t.Fatalf("expected errInvalidJSON for unquoted number, got %v", err)
	}

	// As a struct field.
	type wrap struct {
		ID ID `json:"id"`
	}
	var w wrap
	if err := json.Unmarshal([]byte(`{"id":"42"}`), &w); err != nil || w.ID != 42 {
		t.Fatalf("struct unmarshal: got %d, %v", w.ID, err)
	}
}

func TestIDTextRoundTrip(t *testing.T) {
	id := ID(424242424242)
	b, err := id.MarshalText()
	if err != nil {
		t.Fatalf("marshal text: %v", err)
	}
	var got ID
	if err := got.UnmarshalText(b); err != nil || got != id {
		t.Fatalf("text round-trip: got %d, %v", got, err)
	}
}

func TestParseErrors(t *testing.T) {
	// '0', 'O', 'I', 'l' are not in the base58 alphabet.
	if _, err := ParseBase58([]byte("0OIl")); !errors.Is(err, ErrInvalidBase58) {
		t.Fatalf("expected ErrInvalidBase58, got %v", err)
	}
	// 'A' (capital) is not in the lowercase base32 alphabet.
	if _, err := ParseBase32([]byte("ABV")); !errors.Is(err, ErrInvalidBase32) {
		t.Fatalf("expected ErrInvalidBase32, got %v", err)
	}
	if _, err := ParseString("not-a-number"); err == nil {
		t.Fatalf("expected error parsing non-numeric string")
	}
}
