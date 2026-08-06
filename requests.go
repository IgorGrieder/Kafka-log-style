package main

type sendRequest struct {
	MessageType string `json:"type"`
	Msg         uint64 `json:"msg"`
	Key         string `json:"key"`
}

type sendResponse struct {
	MessageType string `json:"type"`
	Offset      uint64 `json:"offset"`
}
