package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

type SNssai struct {
	Sst int    `json:"sst"`
	Sd  string `json:"sd"`
}

type CreateSessionRequest struct {
	Supi         string  `json:"supi"`
	Gpsi         string  `json:"gpsi"`
	PduSessionId int     `json:"pduSessionId"`
	Dnn          string  `json:"dnn"`
	SNssai       *SNssai `json:"sNssai"`
	ServingNfId  string  `json:"servingNfId"`
	AnType       string  `json:"anType"`
}

type CreateSessionResponse struct {
	SmContextRef string `json:"smContextRef"`
	Supi         string `json:"supi"`
	PduSessionId int    `json:"pduSessionId"`
	HandledBy    string `json:"handledBy"`
	Status       string `json:"status"`
}

var activeRequests int32

func pduSessionHandler(w http.ResponseWriter, r *http.Request, instanceName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	atomic.AddInt32(&activeRequests, 1)
	defer atomic.AddInt32(&activeRequests, -1)

	// Parse JSON body
	var req CreateSessionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"erroe": "Invalid JSON"}`, http.StatusBadRequest)
		return
	}

	// Validate
	if req.Supi == "" || req.PduSessionId == 0 || req.Dnn == "" || req.SNssai == nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"status": "ERROR", "cause": "MANDATORY_IE_MISSING"}`))
		return
	}

	// Sinh session context va luu vao database
	// Tam thoi gia lap
	time.Sleep(15 * time.Millisecond)

	// Tra response ve gateway
	response := CreateSessionResponse{
		SmContextRef: fmt.Sprintf("http://gw/nsmf-pdusession/v1/sm-contexts/ctx-%d", req.PduSessionId),
		Supi:         req.Supi,
		PduSessionId: req.PduSessionId,
		HandledBy:    instanceName,
		Status:       "ACTIVE",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func main() {
	port := flag.String("port", "8081", "Cổng để chạy server")
	flag.Parse()

	instanceName := "pdu-session-" + *port

	http.HandleFunc("/nsmf-pdusession/v1/sm-contexts", func(w http.ResponseWriter, r *http.Request) {
		pduSessionHandler(w, r, instanceName)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	address := ":" + *port

	server := &http.Server{
		Addr:    address,
		Handler: nil,
	}

	server.Protocols = new(http.Protocols)
	server.Protocols.SetHTTP1(true)
	server.Protocols.SetUnencryptedHTTP2(true)

	fmt.Printf("PDU Backend đang chạy tại cổng%s\n", address)
	log.Fatal(server.ListenAndServe())
}
