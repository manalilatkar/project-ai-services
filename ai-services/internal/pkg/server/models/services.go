package models

type Service struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

type GetServicesResp struct {
	Services []Service `json:"services"`
}
