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

func main() {
	gatewayBase := os.Getenv("GATEWAY_URL")
	if gatewayBase == "" {
		gatewayBase = "http://localhost:8000"
	}
	gatewayURL := gatewayBase + "/nsmf-pdusession/v1/sm-contexts"

	numWorkers := 100        // Chay 100 luong dong thoi
	requestsPerWorker := 500 // Moi luong gui 500 request
	numClients := 8

	fmt.Printf("Bắt đầu bài test tải: %d luồng x %d requests = %d tổng requests\n", numWorkers, requestsPerWorker, numWorkers*requestsPerWorker)
	startTime := time.Now()

	var wg sync.WaitGroup
	var successCount int32
	var errorCount int32

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

	// client := &http.Client{
	// 	Timeout: time.Second * 10,
	// 	Transport: &http2.Transport{
	// 		AllowHTTP: true,
	// 		DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
	// 			return net.Dial(network, addr)
	// 		},
	// 	},
	// }

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			client := clientPool[workerID%numClients]

			// Buffer rieng cho tung worker
			buf := make([]byte, 0, len(jsonPrefix)+10+len(jsonSuffix))

			for j := 0; j < requestsPerWorker; j++ {
				id := (workerID * requestsPerWorker) + j

				buf = buf[:0]
				buf = append(buf, jsonPrefix...)
				buf = appendZeroPaddedInt(buf, id, 10)
				buf = append(buf, jsonSuffix...)

				req, _ := http.NewRequest("POST", gatewayURL, bytes.NewReader(buf))
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
						atomic.AddInt32(&successCount, 1)
					} else {
						atomic.AddInt32(&errorCount, 1)
					}
				} else {
					atomic.AddInt32(&errorCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	fmt.Printf("\n--- KẾT QUẢ LOAD TEST ---\n")
	fmt.Printf("Thời gian hoàn thành: %v\n", duration)
	fmt.Printf("Số Request thành công: %d\n", successCount)
	fmt.Printf("Số Request lỗi: %d\n", errorCount)
	fmt.Printf("Tốc độ trung bình (TPS): %.2f requests/giây\n", float64(numWorkers*requestsPerWorker)/duration.Seconds())
}
