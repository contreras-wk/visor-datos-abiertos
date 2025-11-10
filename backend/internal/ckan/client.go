package ckan

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

type Resource struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	URL          string `json:"url"`
	Format       string `json:"format"`
	Description  string `json:"description"`
	Created      string `json:"created"`
	LastModified string `json:"last_modified"`
	Size         int64  `json:"size"`
}

type Package struct {
	ID          string     `json:"id`
	Name        string     `json:"name"`
	Title       string     `json:"title"`
	Description string     `json:"description"`
	Resources   []Resource `json:"resources"`
}

func (c *Client) GetResource(ctx context.Context, resourceID string) (*Resource, error) {
	url := fmt.Sprintf("%sresource_show?id=%s", c.baseURL, resourceID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("CKAN API error: status &d", resp.StatusCode)
	}

	var result struct {
		Success bool     `json:"success"`
		Result  Resource `json:"result"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	if !result.Success {
		return nil, fmt.Errorf("CKAN API returned success=false")
	}

	return &result.Result, nil
}

func (c *Client) GetPackage(ctx context.Context, packageID string) (*Package, error) {
	url := fmt.Sprintf("%spackage_show?id%s", c.baseURL, packageID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Success bool    `json:"success"`
		Result  Package `json:"result"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	if !result.Success {
		return nil, fmt.Errorf("CKAN API returned success=false")
	}

	return &result.Result, nil
}
