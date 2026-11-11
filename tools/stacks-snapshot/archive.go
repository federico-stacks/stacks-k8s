package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
)

// counter counts bytes written through it.
type counter struct {
	w io.Writer
	n int64
}

func (c *counter) Write(b []byte) (int, error) {
	n, err := c.w.Write(b)
	c.n += int64(n)
	return n, err
}

// rewrite copies a tar stream of an actor's data root into a gzip tar archive and returns the
// archive's sha256 and size. It drops the root entry and the peer and mempool databases, which
// describe the source network's peers and pending transactions, not its chain. Members must
// stay inside the data root.
func rewrite(src io.Reader, dst io.Writer) (string, int64, error) {
	sum := sha256.New()
	out := &counter{w: io.MultiWriter(dst, sum)}
	zw := gzip.NewWriter(out)
	tw := tar.NewWriter(zw)
	tr := tar.NewReader(src)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", 0, fmt.Errorf("read data stream: %w", err)
		}
		name, keep, err := memberName(hdr)
		if err != nil {
			return "", 0, err
		}
		if !keep {
			continue
		}
		hdr.Name = name
		if err = tw.WriteHeader(hdr); err != nil {
			return "", 0, err
		}
		if hdr.Typeflag == tar.TypeReg {
			// #nosec G110 -- the input is an uncompressed tar; each copy is bounded by its header size.
			if _, err = io.Copy(tw, tr); err != nil {
				return "", 0, fmt.Errorf("copy %s: %w", name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		return "", 0, err
	}
	if err := zw.Close(); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(sum.Sum(nil)), out.n, nil
}

// memberName returns a member's name relative to the data root and whether to keep it.
func memberName(hdr *tar.Header) (string, bool, error) {
	name := path.Clean(strings.TrimPrefix(hdr.Name, "./"))
	if name == "." {
		return "", false, nil
	}
	if escapes(name) {
		return "", false, fmt.Errorf("member %q leaves the data root", hdr.Name)
	}
	switch hdr.Typeflag {
	case tar.TypeDir:
		return name + "/", true, nil
	case tar.TypeSymlink:
		// #nosec G305 -- this join resolves the link target only to refuse targets outside the root.
		if path.IsAbs(hdr.Linkname) || escapes(path.Join(path.Dir(name), hdr.Linkname)) {
			return "", false, fmt.Errorf("symlink %q leaves the data root", hdr.Name)
		}
		return name, true, nil
	case tar.TypeReg:
		base := path.Base(name)
		if base == "peer.sqlite" || strings.HasPrefix(base, "peer.sqlite-") ||
			strings.HasPrefix(base, "mempool.sqlite") {
			return "", false, nil
		}
		return name, true, nil
	}
	return "", false, fmt.Errorf("member %q has unsupported type %q", hdr.Name, hdr.Typeflag)
}

func escapes(name string) bool {
	return path.IsAbs(name) || name == ".." || strings.HasPrefix(name, "../")
}
