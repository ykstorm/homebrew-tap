// Package chunk splits a byte stream into fixed-size chunks. Fixed-size is
// intentional for v1 (YAGNI); content-defined chunking can replace it later
// behind the same Split signature.
package chunk

import "io"

// Size is the chunk size in bytes (1 MiB).
const Size = 1 << 20

// Split reads r fully and returns its content as successive chunks of up to
// Size bytes. The final chunk may be smaller. An empty reader yields no chunks.
// Each returned slice owns its bytes (safe to retain).
func Split(r io.Reader) ([][]byte, error) {
	var chunks [][]byte
	buf := make([]byte, Size)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			c := make([]byte, n)
			copy(c, buf[:n])
			chunks = append(chunks, c)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return chunks, nil
		}
		if err != nil {
			return nil, err
		}
	}
}
