
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
)

const (
	defaultBaseURL = "https://api.nodeobjects.com/"
	posterBaseURL  = "https://static.nodeobjects.com/thumbnail/"
)

type Config struct {
	BaseURL  string
	Timeout  time.Duration
	Retries  int
	CacheTTL time.Duration
}

type Client struct {
	httpClient *http.Client
	config     Config
	cache      map[string]cacheEntry
	mu         sync.RWMutex
}

type cacheEntry struct {
	Data      json.RawMessage
	ExpiresAt time.Time
}

type APIError struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
}

type SearchItem struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Thumb    string `json:"thumb"`
	Year     string `json:"year"`
	Category string `json:"category"`
	Content  string `json:"content"`
}

type SearchResponse struct {
	Items    []SearchItem `json:"items"`
	Page     int          `json:"page"`
	LastPage int          `json:"last_page,omitempty"`
	NextPage int          `json:"next_page,omitempty"`
}

type Season struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	RIP        string `json:"rip"`
	Server     string `json:"server,omitempty"`
	TitleID    string `json:"title_id"`
	Identifier string `json:"identifier"`
}

type Episode struct {
	Title   string `json:"title"`
	RIP     string `json:"rip"`
	FTitle  string `json:"f_title"`
	FID     string `json:"fid"`
	Disk    string `json:"disk"`
	Quality string `json:"quality"`
	Size    string `json:"size"`
	Number  int    `json:"number"`
}

type FileInfo struct {
	URL      string `json:"url"`
	Filename string `json:"filename"`
	Size     string `json:"size"`
	Host     string `json:"host"`
	IsDL     bool   `json:"isDL"`
}

type Source struct {
	Provider string `json:"provider"`
	Title    string `json:"title"`
	URL      string `json:"url"`
	Filename string `json:"filename"`
	Quality  string `json:"quality,omitempty"`
	Size     string `json:"size,omitempty"`
	Host     string `json:"host,omitempty"`
	IsDirect bool   `json:"is_direct"`
}

func NewClient() *Client {
	base := strings.TrimSpace(os.Getenv("DRAMACHI_BASE_URL"))
	if base == "" {
		base = defaultBaseURL
	}

	timeout := envInt("REQUEST_TIMEOUT", 20)
	retries := envInt("REQUEST_RETRIES", 2)
	cacheTTL := envInt("CACHE_TTL", 60)

	return &Client{
		httpClient: &http.Client{
			Timeout: time.Duration(timeout) * time.Second,
		},
		config: Config{
			BaseURL:  strings.TrimRight(base, "/") + "/",
			Timeout:  time.Duration(timeout) * time.Second,
			Retries:  retries,
			CacheTTL: time.Duration(cacheTTL) * time.Second,
		},
		cache: make(map[string]cacheEntry),
	}
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func (c *Client) cacheGet(key string) (json.RawMessage, bool) {
	c.mu.RLock()
	entry, ok := c.cache[key]
	c.mu.RUnlock()

	if !ok || time.Now().After(entry.ExpiresAt) {
		return nil, false
	}
	return entry.Data, true
}

func (c *Client) cacheSet(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.cache) > 500 {
		for k, v := range c.cache {
			if time.Now().After(v.ExpiresAt) {
				delete(c.cache, k)
			}
		}
	}

	c.cache[key] = cacheEntry{
		Data:      append(json.RawMessage(nil), data...),
		ExpiresAt: time.Now().Add(c.config.CacheTTL),
	}
}

func (c *Client) request(ctx context.Context, params url.Values) ([]byte, error) {
	endpoint := c.config.BaseURL + "?" + params.Encode()

	cacheKeyBytes := sha256.Sum256([]byte(endpoint))
	cacheKey := hex.EncodeToString(cacheKeyBytes[:])

	if cached, ok := c.cacheGet(cacheKey); ok {
		return cached, nil
	}

	var lastErr error

	for attempt := 0; attempt <= c.config.Retries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "Prophecy-Dramachi-Server/1.0")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
		} else {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
			resp.Body.Close()

			if readErr != nil {
				lastErr = readErr
			} else if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				c.cacheSet(cacheKey, body)
				return body, nil
			} else {
				lastErr = fmt.Errorf("upstream returned HTTP %d", resp.StatusCode)
				if resp.StatusCode != http.StatusTooManyRequests &&
					resp.StatusCode < 500 {
					return nil, lastErr
				}
			}
		}

		if attempt < c.config.Retries {
			delay := time.Duration(attempt+1) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	return nil, lastErr
}

func decodeJSON[T any](data []byte) (T, error) {
	var result T
	err := json.Unmarshal(data, &result)
	return result, err
}

func (c *Client) upstream(ctx context.Context, params url.Values) (map[string]json.RawMessage, error) {
	data, err := c.request(ctx, params)
	if err != nil {
		return nil, err
	}

	var result map[string]json.RawMessage
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func rawArray(data json.RawMessage) []json.RawMessage {
	var items []json.RawMessage
	_ = json.Unmarshal(data, &items)
	return items
}

func rawString(data json.RawMessage) string {
	var value string
	_ = json.Unmarshal(data, &value)
	return value
}

func firstString(object map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		if value := rawString(object[key]); value != "" {
			return value
		}
	}
	return ""
}

func (c *Client) search(ctx context.Context, query string, page int) (SearchResponse, error) {
	params := url.Values{
		"interface": {"search"},
		"q":         {query},
		"filter":    {"all"},
		"page":      {strconv.Itoa(page)},
	}

	result, err := c.upstream(ctx, params)
	if err != nil {
		return SearchResponse{}, err
	}

	var items []SearchItem
	for _, raw := range rawArray(result["data"]) {
		var item SearchItem
		if err := json.Unmarshal(raw, &item); err != nil {
			continue
		}
		if item.ID == "" {
			item.ID = firstString(map[string]json.RawMessage{
				"id": raw,
			}, "id")
		}
		if item.Thumb != "" && !strings.HasPrefix(item.Thumb, "http") {
			item.Thumb = posterBaseURL + strings.TrimLeft(item.Thumb, "/")
		}
		items = append(items, item)
	}

	var lastPage, nextPage int
	_ = json.Unmarshal(result["last_page"], &lastPage)
	_ = json.Unmarshal(result["next_page"], &nextPage)

	return SearchResponse{
		Items:    items,
		Page:     page,
		LastPage: lastPage,
		NextPage: nextPage,
	}, nil
}

func (c *Client) details(ctx context.Context, id string) (map[string]json.RawMessage, error) {
	return c.upstream(ctx, url.Values{
		"interface": {"title_v2"},
		"id":        {id},
	})
}

func (c *Client) seasons(ctx context.Context, id string) ([]Season, error) {
	details, err := c.details(ctx, id)
	if err != nil {
		return nil, err
	}

	var album map[string]json.RawMessage
	_ = json.Unmarshal(details["album"], &album)

	rawSeasons := rawArray(details["seasons"])
	if len(rawSeasons) == 0 {
		rawSeasons = rawArray(details["data"])
	}

	var seasons []Season

	for _, raw := range rawSeasons {
		var item map[string]json.RawMessage
		if json.Unmarshal(raw, &item) != nil {
			continue
		}

		rip := firstString(item, "rip")
		if rip == "" {
			continue
		}

		name := firstString(item, "season_name", "name")
		version := firstString(item, "version_name", "version")

		seasons = append(seasons, Season{
			Name:       name,
			Version:    version,
			RIP:        rip,
			Server:     firstString(item, "server"),
			TitleID:    id,
			Identifier: id + "::" + rip,
		})
	}

	return seasons, nil
}

func (c *Client) episodes(ctx context.Context, id, rip string) ([]Episode, error) {
	params := url.Values{
		"interface": {"eplist"},
		"id":        {id},
		"season":    {rip},
	}

	result, err := c.upstream(ctx, params)
	if err != nil {
		return nil, err
	}

	rawEpisodes := rawArray(result["data"])
	if len(rawEpisodes) == 0 {
		rawEpisodes = rawArray(result["episodes"])
	}

	episodes := make([]Episode, 0, len(rawEpisodes))

	for index, raw := range rawEpisodes {
		var item map[string]json.RawMessage
		if json.Unmarshal(raw, &item) != nil {
			continue
		}

		episode := Episode{
			Title:   firstString(item, "title"),
			RIP:     firstString(item, "rip"),
			FTitle:  firstString(item, "f_title"),
			FID:     firstString(item, "fid"),
			Disk:    firstString(item, "disk"),
			Quality: firstString(item, "quality"),
			Size:    firstString(item, "size"),
		}

		episode.Number = parseEpisodeNumber(episode.FTitle, episode.Title)
		if episode.Number == 0 {
			episode.Number = index + 1
		}

		episodes = append(episodes, episode)
	}

	sort.SliceStable(episodes, func(i, j int) bool {
		return episodes[i].Number < episodes[j].Number
	})

	return episodes, nil
}

func parseEpisodeNumber(values ...string) int {
	for _, value := range values {
		lower := strings.ToLower(value)
		for _, marker := range []string{"episode", "ep", "e"} {
			index := strings.Index(lower, marker)
			if index < 0 {
				continue
			}

			start := index + len(marker)
			for start < len(value) && (value[start] == ' ' || value[start] == '-' || value[start] == '_') {
				start++
			}

			end := start
			for end < len(value) && value[end] >= '0' && value[end] <= '9' {
				end++
			}

			if end > start {
				number, _ := strconv.Atoi(value[start:end])
				return number
			}
		}
	}
	return 0
}

func (c *Client) resolve(ctx context.Context, id string, episode Episode) ([]Source, error) {
	if episode.FID == "" || episode.Disk == "" {
		return nil, errors.New("episode is missing fid or disk")
	}

	result, err := c.upstream(ctx, url.Values{
		"interface": {"getFile"},
		"fid":       {episode.FID},
		"findex":    {episode.Disk},
	})
	if err != nil {
		return nil, err
	}

	rawFiles := rawArray(result["fileInfo"])
	var sources []Source

	for _, raw := range rawFiles {
		var file FileInfo
		if json.Unmarshal(raw, &file) != nil {
			continue
		}

		parsed, err := url.Parse(file.URL)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
			continue
		}

		sources = append(sources, Source{
			Provider: "dramachi",
			Title:    episode.Title,
			URL:      file.URL,
			Filename: file.Filename,
			Quality:  episode.Quality,
			Size:     file.Size,
			Host:     file.Host,
			IsDirect: file.IsDL,
		})
	}

	return sources, nil
}

func responseError(c *fiber.Ctx, status int, message string) error {
	return c.Status(status).JSON(APIError{
		Status:  status,
		Message: message,
	})
}

func main() {
	client := NewClient()
	app := fiber.New(fiber.Config{
		AppName:      "Dramachi Prophecy Sub Server",
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	})

	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"service": "dramachi-prophecy-sub-server",
			"status":  "online",
		})
	})

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})

	app.Get("/api/v1/search", func(c *fiber.Ctx) error {
		query := strings.TrimSpace(c.Query("q"))
		page := c.QueryInt("page", 1)

		if query == "" {
			return responseError(c, 400, "query parameter q is required")
		}
		if page < 1 {
			page = 1
		}

		result, err := client.search(c.UserContext(), query, page)
		if err != nil {
			log.Printf("search error: %v", err)
			return responseError(c, 502, "upstream search failed")
		}
		return c.JSON(fiber.Map{
			"status": "success",
			"data":   result,
		})
	})

	app.Get("/api/v1/title/:id", func(c *fiber.Ctx) error {
		id := strings.TrimSpace(c.Params("id"))
		if id == "" {
			return responseError(c, 400, "title id is required")
		}

		details, err := client.details(c.UserContext(), id)
		if err != nil {
			log.Printf("details error: %v", err)
			return responseError(c, 502, "upstream details failed")
		}

		seasons, seasonErr := client.seasons(c.UserContext(), id)
		if seasonErr != nil {
			log.Printf("season error: %v", seasonErr)
		}

		return c.JSON(fiber.Map{
			"status":  "success",
			"data":    details,
			"seasons": seasons,
		})
	})

	app.Get("/api/v1/title/:id/seasons", func(c *fiber.Ctx) error {
		seasons, err := client.seasons(c.UserContext(), c.Params("id"))
		if err != nil {
			return responseError(c, 502, "upstream seasons failed")
		}
		return c.JSON(fiber.Map{
			"status": "success",
			"data":   seasons,
		})
	})

	app.Get("/api/v1/title/:id/seasons/:rip/episodes", func(c *fiber.Ctx) error {
		episodes, err := client.episodes(
			c.UserContext(),
			c.Params("id"),
			c.Params("rip"),
		)
		if err != nil {
			return responseError(c, 502, "upstream episodes failed")
		}

		return c.JSON(fiber.Map{
			"status": "success",
			"data":   episodes,
		})
	})

	app.Get("/api/v1/title/:id/seasons/:rip/episodes/:number/sources", func(c *fiber.Ctx) error {
		number := c.Params("number")
		episodeNumber, err := strconv.Atoi(number)
		if err != nil || episodeNumber < 1 {
			return responseError(c, 400, "invalid episode number")
		}

		episodes, err := client.episodes(
			c.UserContext(),
			c.Params("id"),
			c.Params("rip"),
		)
		if err != nil {
			return responseError(c, 502, "upstream episodes failed")
		}

		var selected *Episode
		for i := range episodes {
			if episodes[i].Number == episodeNumber {
				selected = &episodes[i]
				break
			}
		}

		if selected == nil {
			return responseError(c, 404, "episode not found")
		}

		sources, err := client.resolve(c.UserContext(), c.Params("id"), *selected)
		if err != nil {
			log.Printf("source resolve error: %v", err)
			return responseError(c, 502, "upstream source lookup failed")
		}

		return c.JSON(fiber.Map{
			"status":   "success",
			"provider": "dramachi",
			"action":   "sources",
			"data": fiber.Map{
				"episode": *selected,
				"sources": sources,
			},
		})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "10000"
	}

	log.Printf("Dramachi server starting on :%s", port)
	log.Fatal(app.Listen(":" + port))
}
