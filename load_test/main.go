package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
)

func main() {
	gatewayURL := "http://localhost:8000/nsmf-pdusession/v1/sm-contexts"

	numWorkers := 100        // Chay 100 luong dong thoi
	requestsPerWorker := 500 // Moi luong gui 500 request

	fmt.Printf("Bắt đầu bài test tải: %d luồng x %d requests = %d tổng requests\n", numWorkers, requestsPerWorker, numWorkers*requestsPerWorker)
	startTime := time.Now()

	var wg sync.WaitGroup
	var successCount int32
	var errorCount int32

	client := &http.Client{
		Timeout: time.Second * 10,
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
	}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < requestsPerWorker; j++ {
				supi := fmt.Sprintf("imsi-%010d", (workerID*requestsPerWorker)+j)
				body := fmt.Appendf(nil, `{
					"supi": "%s",
					"gpsi": "msisdn-84900000001",
					"pduSessionId": 1,
					"dnn": "v-internet",
					"sNssai": { "sst": 1, "sd": "000001" },
					"servingNfId": "2ab2b5a9-68e8-4ee6-b939-024c109b520c",
					"anType": "3GPP_ACCESS"
				}`, supi)

				req, _ := http.NewRequest("POST", gatewayURL, bytes.NewBuffer(body))
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				if err == nil {
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
