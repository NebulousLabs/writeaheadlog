package writeaheadlog

import (
	"testing"

	"github.com/NebulousLabs/fastrand"
)

// TestPageMarshalling checks that pages can be marshalled and unmarshalled correctly
func TestPageMarshalling(t *testing.T) {
	t.Skip("not implemented")
}

// BenchmarkPageAppendTo benchmarks the appendTo method of page.
func BenchmarkPageAppendTo(b *testing.B) {
	p := page{
		offset:  4096,
		payload: fastrand.Bytes(MaxPayloadSize), // ensure marshalled size is 4096 bytes
		nextPage: &page{
			offset: 12345,
		},
	}
	buf := make([]byte, pageSize)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		p.appendTo(buf[:0])
	}
}
