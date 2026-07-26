package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
)

var counter uint64

func main() {
	// Khai bao danh sach 3 backend
	backendURLs := []string{
		"http://localhost:8081",
		"http://localhost:8082",
		"http://localhost:8083",
	}

	// Tao 3 Reverse Proxy tuong ung
	var proxies []*httputil.ReverseProxy
	for _, u := range backendURLs {
		parsedUrl, _ := url.Parse(u)
		proxies = append(proxies, httputil.NewSingleHostReverseProxy(parsedUrl))
	}

	// Ham xu ly request (Round Robin)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Tang bien dem len 1 cach an toan
		currentCount := atomic.AddUint64(&counter, 1)

		index := int(currentCount % uint64(len(proxies)))

		fmt.Printf("[Gateway] Request #%d -> Chuyển tới Backend cổng %s\n", currentCount, backendURLs[index])

		// Forward request toi backend da chon
		proxies[index].ServeHTTP(w, r)
	})

	port := ":8000"
	fmt.Printf("Gateway đang chạy tại cổng %s (Thuật toán: Round Robin)\n", port)
	log.Fatal(http.ListenAndServe(port, nil))
}
