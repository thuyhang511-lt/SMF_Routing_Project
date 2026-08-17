package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
)

// jsonPrefix va jsonSuffix la 2 phan TINH cua body JSON, khong doi giua cac
// request - chi co doan "imsi-XXXXXXXXXX" (10 chu so) o giua la thay doi.
// Tach san thanh []byte 1 LAN duy nhat (khong phai moi request), tranh
// fmt.Sprintf/fmt.Appendf phai parse lai format string + cap phat moi lan.
var (
	jsonPrefix = []byte(`{"supi":"imsi-`)
	jsonSuffix = []byte(`","gpsi":"msisdn-84900000001","pduSessionId":1,"dnn":"v-internet","sNssai":{"sst":1,"sd":"000001"},"servingNfId":"2ab2b5a9-68e8-4ee6-b939-024c109b520c","anType":"3GPP_ACCESS"}`)
)

// appendZeroPaddedInt ghi truc tiep so nguyen (dang thap phan, dem 0 dau du
// "width" chu so) vao cuoi buf - khong qua fmt (khong reflection, khong
// cap phat string trung gian nhu fmt.Sprintf("%010d", n)).
func appendZeroPaddedInt(buf []byte, n int, width int) []byte {
	var tmp [20]byte
	i := len(tmp)
	if n == 0 {
		i--
		tmp[i] = '0'
	}
	for n > 0 {
		i--
		tmp[i] = byte('0' + n%10)
		n /= 10
	}
	for pad := width - (len(tmp) - i); pad > 0; pad-- {
		buf = append(buf, '0')
	}
	return append(buf, tmp[i:]...)
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func buildBody(buf []byte, id int) []byte {
	buf = buf[:0]
	buf = append(buf, jsonPrefix...)
	buf = appendZeroPaddedInt(buf, id, 10)
	buf = append(buf, jsonSuffix...)
	return buf
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p/100*float64(len(sorted))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func runLoad(gatewayURL string, clientPool []*http.Client, numWorkers, requestsPerWorker int, warmup bool) (sucess int64, errConn, err249, err500, err503, errOther int64, latencies []time.Duration) {
	var wg sync.WaitGroup
	var successCount, cConn, c429, c500, c503, cOther int64

	latSlices := make([][]time.Duration, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			client := clientPool[workerID%len(clientPool)]
			buf := make([]byte, 0, len(jsonPrefix)+10+len(jsonSuffix))

			var localLat []time.Duration
			if !warmup {
				localLat = make([]time.Duration, 0, requestsPerWorker)
			}

			for j := 0; j < requestsPerWorker; j++ {
				id := (workerID * requestsPerWorker) + j
				buf = buildBody(buf, id)

				req, _ := http.NewRequest("POST", gatewayURL, bytes.NewReader(buf))

				req.Header.Set("Content-Type", "application/json")

				start := time.Now()
				resp, reqErr := client.Do(req)
				elapsed := time.Since(start)

				if reqErr != nil {
					atomic.AddInt64(&cConn, 1)
					continue
				}
				io.Copy(io.Discard, req.Body)
				req.Body.Close()

				switch resp.StatusCode {
				case http.StatusOK, http.StatusCreated:
					atomic.AddInt64(&successCount, 1)
					if !warmup {
						localLat = append(localLat, elapsed)
					}
				case http.StatusTooManyRequests:
					atomic.AddInt64(&c429, 1)
				case http.StatusInternalServerError:
					atomic.AddInt64(&c500, 1)
				case http.StatusServiceUnavailable:
					atomic.AddInt64(&c503, 1)
				default:
					atomic.AddInt64(&cOther, 1)
				}
			}
			latSlices[workerID] = localLat
		}(i)
	}
	wg.Wait()

	if !warmup {
		for _, s := range latSlices {
			latencies = append(latencies, s...)
		}
	}
	return successCount, cConn, c429, c500, c503, cOther, latencies
}

func main() {
	gatewayBase := os.Getenv("GATEWAY_URL")
	if gatewayBase == "" {
		gatewayBase = "http://localhost:8000"
	}
	gatewayURL := gatewayBase + "/nsmf-pdusession/v1/sm-contexts"

	numWorkers := getEnvInt("WORKERS", 100)
	requestsPerWorker := getEnvInt("REQUESTS_PER_WORKER", 500)
	warmupRequests := getEnvInt("WARMUP_REQUEST", 1000)
	numClients := getEnvInt("NUM_CLIENT", 8)

	clientPool := make([]*http.Client, numClients)
	for i := range clientPool {
		clientPool[i] = &http.Client{
			Timeout: time.Second * 10,
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr)
				},
			},
		}
	}

	// --- WARM-UP: khong tinh vao ket qua, chi de "lam am" ket noi
	// TCP/H2C, connection pool cua pgx, cache noi bo cua Go runtime ---
	if warmupRequests > 0 {
		warmupWorkers := numWorkers
		warmupPerWorker := warmupRequests / warmupWorkers
		if warmupPerWorker < 1 {
			warmupPerWorker = 1
		}
		fmt.Printf("Đang warm-up (%d request, không tính vào kết quả)...\n", warmupWorkers*warmupPerWorker)
		runLoad(gatewayURL, clientPool, warmupWorkers, warmupPerWorker, true)
		fmt.Println("Warm-up xong. Bắt đầu đo thật...")
	}

	// --- ĐO THẬT ---
	fmt.Printf("Bắt đầu bài test tải: %d luồng x %d requests = %d tổng requests (%d client)\n",
		numWorkers, requestsPerWorker, numWorkers*requestsPerWorker, numClients)
	startTime := time.Now()

	success, errConn, err429, err500, err503, errOther, latencies := runLoad(gatewayURL, clientPool, numWorkers, requestsPerWorker, false)

	duration := time.Since(startTime)
	totalErr := errConn + err429 + err500 + err503 + errOther
	totalReq := numWorkers * requestsPerWorker
	tps := float64(totalReq) / duration.Seconds()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)

	fmt.Printf("\n--- KẾT QUẢ LOAD TEST ---\n")
	fmt.Printf("Thời gian hoàn thành: %v\n", duration)
	fmt.Printf("Số Request thành công: %d\n", success)
	fmt.Printf("Số Request lỗi: %d (kết nối=%d, 429=%d, 500=%d, 503=%d, khác=%d)\n",
		totalErr, errConn, err429, err500, err503, errOther)
	fmt.Printf("Tốc độ trung bình (TPS): %.2f requests/giây\n", tps)
	fmt.Printf("Độ trễ p50 / p95 / p99: %.2fms / %.2fms / %.2fms\n",
		float64(p50.Microseconds())/1000, float64(p95.Microseconds())/1000, float64(p99.Microseconds())/1000)

	// Khoi RESULT_ machine-readable de script benchmark ben ngoai (PowerShell)
	// grep ra tinh median qua nhieu lan chay - khong can parse cau tieng Viet.
	fmt.Printf("\nRESULT_TPS=%.4f\n", tps)
	fmt.Printf("RESULT_P50_MS=%.4f\n", float64(p50.Microseconds())/1000)
	fmt.Printf("RESULT_P95_MS=%.4f\n", float64(p95.Microseconds())/1000)
	fmt.Printf("RESULT_P99_MS=%.4f\n", float64(p99.Microseconds())/1000)
	fmt.Printf("RESULT_SUCCESS=%d\n", success)
	fmt.Printf("RESULT_ERROR=%d\n", totalErr)
	fmt.Printf("RESULT_ERR_CONN=%d\n", errConn)
	fmt.Printf("RESULT_ERR_429=%d\n", err429)
	fmt.Printf("RESULT_ERR_500=%d\n", err500)
	fmt.Printf("RESULT_ERR_503=%d\n", err503)
}
