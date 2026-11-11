package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"strings"
	"testing"
)

type entry struct {
	name, link string
	kind       byte
	body       string
}

func tarOf(t *testing.T, entries ...entry) *bytes.Buffer {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	for _, e := range entries {
		hdr := &tar.Header{Name: e.name, Linkname: e.link, Typeflag: e.kind, Mode: 0o644, Size: int64(len(e.body))}
		if e.kind != tar.TypeReg {
			hdr.Size = 0
		}
		if err := w.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if e.kind == tar.TypeReg {
			if _, err := w.Write([]byte(e.body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// tar pads its output; rewrite must stop at the end-of-archive marker.
	b.Write(make([]byte, 10240))
	return &b
}

func members(t *testing.T, archive []byte) map[string]string {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	r := tar.NewReader(zr)
	out := map[string]string{}
	for {
		hdr, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		out[hdr.Name] = string(body)
	}
}

func TestRewriteKeepsChainDataAndDropsPeersMempoolsAndRoot(t *testing.T) {
	src := tarOf(t,
		entry{name: "./", kind: tar.TypeDir},
		entry{name: "./chainstate/", kind: tar.TypeDir},
		entry{name: "./chainstate/vm/index.sqlite", kind: tar.TypeReg, body: "marf"},
		entry{name: "./peer.sqlite", kind: tar.TypeReg, body: "peers"},
		entry{name: "./peer.sqlite-wal", kind: tar.TypeReg, body: "wal"},
		entry{name: "./chainstate/mempool.sqlite", kind: tar.TypeReg, body: "pending"},
		entry{name: "./chainstate/mempool.sqlite-shm", kind: tar.TypeReg, body: "shm"},
		entry{name: "./regtest/blocks/blk00000.dat", kind: tar.TypeReg, body: "blocks"},
		entry{name: "./current", link: "regtest/blocks", kind: tar.TypeSymlink},
	)
	var dst bytes.Buffer
	sum, size, err := rewrite(src, &dst)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(dst.Bytes())
	if sum != hex.EncodeToString(digest[:]) || size != int64(dst.Len()) {
		t.Fatalf("checksum %s/%d does not describe the written archive", sum, size)
	}
	got := members(t, dst.Bytes())
	names := make([]string, 0, len(got))
	for name := range got {
		names = append(names, name)
	}
	sort.Strings(names)
	want := "chainstate/,chainstate/vm/index.sqlite,current,regtest/blocks/blk00000.dat"
	if strings.Join(names, ",") != want {
		t.Fatalf("members %v, want %s", names, want)
	}
	if got["chainstate/vm/index.sqlite"] != "marf" || got["regtest/blocks/blk00000.dat"] != "blocks" {
		t.Fatalf("contents changed: %v", got)
	}
}

func TestRewriteRejectsMembersOutsideTheDataRoot(t *testing.T) {
	for name, e := range map[string]entry{
		"parent":            {name: "../etc/passwd", kind: tar.TypeReg, body: "x"},
		"nested parent":     {name: "./a/../../b", kind: tar.TypeReg, body: "x"},
		"absolute":          {name: "/etc/passwd", kind: tar.TypeReg, body: "x"},
		"absolute link":     {name: "./l", link: "/etc", kind: tar.TypeSymlink},
		"escaping link":     {name: "./a/l", link: "../../etc", kind: tar.TypeSymlink},
		"hard link":         {name: "./h", link: "chainstate", kind: tar.TypeLink},
		"device":            {name: "./dev", kind: tar.TypeChar},
		"named pipe (fifo)": {name: "./fifo", kind: tar.TypeFifo},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := rewrite(tarOf(t, e), io.Discard); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestRewriteReportsATruncatedStream(t *testing.T) {
	full := tarOf(t, entry{name: "./a", kind: tar.TypeReg, body: strings.Repeat("x", 4096)}).Bytes()
	if _, _, err := rewrite(bytes.NewReader(full[:1024]), io.Discard); err == nil {
		t.Fatal("truncated stream accepted")
	}
}
