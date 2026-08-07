package main

type sendRequest struct {
	MessageType string `json:"type"`
	Msg         int    `json:"msg"`
	Key         string `json:"key"`
}

type sendResponse struct {
	MessageType string `json:"type"`
	Offset      int    `json:"offset"`
}

type pollRequest struct {
	MessageType string         `json:"type"`
	Offsets     map[string]int `json:"offsets"`
}

type pollResponse struct {
	MessageType string             `json:"type"`
	Messages    map[string][][]int `json:"msgs"`
}

type commitOffsetsRequest struct {
	MessageType string         `json:"type"`
	Offsets     map[string]int `json:"offsets"`
}

type commitOffsetsResponse struct {
	MessageType string `json:"type"`
}

type listCommittedOffsetsRequest struct {
	MessageType string   `json:"type"`
	Keys        []string `json:"keys"`
}

type listCommittedOffsetsResponse struct {
	MessageType string         `json:"type"`
	Offsets     map[string]int `json:"offsets"`
}
