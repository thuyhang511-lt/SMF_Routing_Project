package models

//easyjson:json
type SNssai struct {
	Sst int    `json:"sst"`
	Sd  string `json:"sd"`
}

//easyjson:json
type CreateSessionRequest struct {
	Supi         string  `json:"supi"`
	Gpsi         string  `json:"gpsi"`
	PduSessionId int     `json:"pduSessionId"`
	Dnn          string  `json:"dnn"`
	SNssai       *SNssai `json:"sNssai"`
	ServingNfId  string  `json:"servingNfId"`
	AnType       string  `json:"anType"`
}

//easyjson:json
type CreateSessionResponse struct {
	SmContextRef string `json:"smContextRef"`
	Supi         string `json:"supi"`
	PduSessionId int    `json:"pduSessionId"`
	HandledBy    string `json:"handledBy"`
	Status       string `json:"status"`
}

//easyjson:json
type HeartbeatMsg struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}
