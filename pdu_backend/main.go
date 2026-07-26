package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
)

func main() {
	port := flag.String("port", "8081", "Cổng để chạy server")
	flag.Parse()

	http.HandleFunc("/nsmf-pdusession/v1/sm-contexts", func(w http.ResponseWriter, r *http.Request) {
		instanceName := "pdu-backend-" + *port
		fmt.Printf("[%s] Đã nhận request!\n", instanceName)

		w.Header().Set("Content-Type", "application/json")
		response := map[string]interface{}{
			"status":    "ACTIVE",
			"handledBy": instanceName,
		}
		json.NewEncoder(w).Encode(response)
	})

	address := ":" + *port
	fmt.Printf("PDU Backend đang chạy tại cổng%s\n", address)
	log.Fatal(http.ListenAndServe(address, nil))
}
