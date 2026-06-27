package codec

import "testing"

type order struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

func TestJSONRoundTrip(t *testing.T) {
	in := order{ID: "o1", Total: 99}
	data, err := JSON.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out order
	if err := JSON.Unmarshal(data, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out != in {
		t.Errorf("round trip mismatch: %+v != %+v", out, in)
	}
	if JSON.ContentType() != "application/json" {
		t.Errorf("ContentType = %q", JSON.ContentType())
	}
}

func TestOrDefaultsToJSON(t *testing.T) {
	if Or(nil) != JSON {
		t.Error("Or(nil) should return JSON")
	}
	if Or(JSON) != JSON {
		t.Error("Or(JSON) should return JSON")
	}
}
