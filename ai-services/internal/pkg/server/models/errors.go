package models

type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type ErrorResp struct {
	Error Error `json:"error"`
}
