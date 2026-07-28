package algorithm

import (
	"encoding/json"
	"hash/crc32"
	"hash/fnv"
	"strconv"
)

const M = 1009

type Maglev struct {
	D        []int
	Backends []*Backend
}

func hash1(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func hash2(s string) int {
	return int(crc32.ChecksumIEEE([]byte(s)))
}

func (m *Maglev) Build(healthy []*Backend) {
	N := len(healthy)
	if N == 0 {
		m.D = nil
		m.Backends = nil
		return
	}

	T := make([][]int, N)
	for i, b := range healthy {
		name := b.URL.String()
		offset := hash1(name) % M
		skip := (hash2(name) % (M - 1)) + 1

		T[i] = make([]int, M)
		for j := 0; j < M; j++ {
			T[i][j] = (offset + j*skip) % M
		}
	}

	D := make([]int, M)
	for i := range D {
		D[i] = -1
	}

	next := make([]int, N)
	filled := 0

	for filled < M {
		for i := 0; i < N; i++ {
			for next[i] < M {
				b := T[i][next[i]]
				next[i]++
				if D[b] == -1 {
					D[b] = i
					filled++
					break
				}
			}
		}
	}

	m.D = D
	m.Backends = healthy
}

func (m *Maglev) Next(healthy []*Backend, bodyBytes []byte) *Backend {
	if len(m.D) == 0 || len(m.Backends) == 0 {
		return nil
	}

	var payload struct {
		Supi string `json:"supi"`
	}
	json.Unmarshal(bodyBytes, &payload)
	supi := payload.Supi

	bucketId := 0
	if len(supi) >= 3 {
		last3 := supi[len(supi)-3:]
		val, err := strconv.Atoi(last3)
		if err == nil {
			bucketId = val % M
		} else {
			bucketId = hash1(supi) % M
		}
	} else {
		bucketId = hash1(supi) % M
	}

	pduIndex := m.D[bucketId]
	if pduIndex >= 0 && pduIndex < len(m.Backends) {
		return m.Backends[pduIndex]
	}
	return nil
}
