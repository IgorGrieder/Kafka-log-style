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
