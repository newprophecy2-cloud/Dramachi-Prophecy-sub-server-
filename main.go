package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
)

const (
	defaultBaseURL = "https://api.nodeobjects.com/"
	posterBaseURL  = "https://static.nodeobjects.com/thumbnail/"
	providerID     = "dramachi"
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
	Data      []byte
	ExpiresAt time.Time
}

type ProviderError struct {
	Code      string
	Message   string
	Retryable bool
}

func (e *ProviderError) Error() string {
	return e.Message
}

type EpisodeRef struct {
	TitleID string `json:"t"`
	RIP     string `json:"r"`
	FID     string `json:"f"`
	Disk    string `json:"d"`
	Number  int    `json:"n"`
}

type FileInfo struct {
	URL      string `json:"url"`
	Filename string `json:"filename"`
	Size     string `json:"size"`
	Host     string `json:"host"`
	IsDL     bool   `json:"isDL"`
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

// --------------------------------------------------
// CACHE
// --------------------------------------------------

func (c *Client) cacheGet(key string) ([]byte, bool) {
	c.mu.RLock()
	entry, ok := c.cache[key]
	c.mu.RUnlock()

	if !ok || time.Now().After(entry.ExpiresAt) {
		return nil, false
	}

	return append([]byte(nil), entry.Data...), true
}

func (c *Client) cacheSet(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.cache) > 500 {
		now := time.Now()
		for key, item := range c.cache {
			if now.After(item.ExpiresAt) {
				delete(c.cache, key)
			}
		}
	}

	c.cache[key] = cacheEntry{
		Data:      append([]byte(nil), data...),
		ExpiresAt: time.Now().Add(c.config.CacheTTL),
	}
}

// --------------------------------------------------
// UPSTREAM REQUEST
// --------------------------------------------------

func (c *Client) request(
	ctx context.Context,
	params url.Values,
) ([]byte, error) {
	endpoint := c.config.BaseURL + "?" + params.Encode()

	hash := sha256.Sum256([]byte(endpoint))
	cacheKey := hex.EncodeToString(hash[:])

	if cached, ok := c.cacheGet(cacheKey); ok {
		return cached, nil
	}

	var lastErr error

	for attempt := 0; attempt <= c.config.Retries; attempt++ {
		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			endpoint,
			nil,
		)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "Prophecy-Dramachi-Server/1.0")

		resp, err := c.httpClient.Do(req)

		if err != nil {
			lastErr = err
		} else {
			body, readErr := io.ReadAll(
				io.LimitReader(resp.Body, 8<<20),
			)
			resp.Body.Close()

			if readErr != nil {
				lastErr = readErr
			} else if resp.StatusCode >= 200 &&
				resp.StatusCode < 300 {
				c.cacheSet(cacheKey, body)
				return body, nil
			} else {
				lastErr = fmt.Errorf(
					"upstream HTTP %d",
					resp.StatusCode,
				)

				if resp.StatusCode != 429 &&
					resp.StatusCode < 500 {
					return nil, &ProviderError{
						Code:    "UPSTREAM_HTTP_ERROR",
						Message: lastErr.Error(),
					}
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

	return nil, &ProviderError{
		Code:      "NETWORK_ERROR",
		Message:   fmt.Sprintf("Dramachi request failed: %v", lastErr),
		Retryable: true,
	}
}

func (c *Client) upstream(
	ctx context.Context,
	params url.Values,
) (map[string]json.RawMessage, error) {
	body, err := c.request(ctx, params)
	if err != nil {
		return nil, err
	}

	var result map[string]json.RawMessage
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, &ProviderError{
			Code:    "PARSE_ERROR",
			Message: "Invalid JSON response from Dramachi",
		}
	}

	return result, nil
}

// --------------------------------------------------
// JSON HELPERS
// --------------------------------------------------

func rawArray(data json.RawMessage) []json.RawMessage {
	var result []json.RawMessage
	_ = json.Unmarshal(data, &result)
	return result
}

func rawString(data json.RawMessage) string {
	var result string
	_ = json.Unmarshal(data, &result)
	return result
}

func rawInt(data json.RawMessage) int {
	var result int
	_ = json.Unmarshal(data, &result)
	return result
}

func firstString(
	object map[string]json.RawMessage,
	keys ...string,
) string {
	for _, key := range keys {
		value := rawString(object[key])
		if value != "" {
			return value
		}
	}
	return ""
}

func objectValue(
	data json.RawMessage,
) map[string]json.RawMessage {
	var result map[string]json.RawMessage
	_ = json.Unmarshal(data, &result)
	return result
}

func parseNumber(values ...string) int {
	re := regexp.MustCompile(`(?i)(?:episode|ep|e)[\s._-]*(\d+)`)

	for _, value := range values {
		match := re.FindStringSubmatch(value)
		if len(match) == 2 {
			number, _ := strconv.Atoi(match[1])
			return number
		}
	}

	return 0
}

func seasonNumber(name string, fallback int) int {
	re := regexp.MustCompile(`\d+`)
	match := re.FindString(name)

	if match == "" {
		return fallback
	}

	number, err := strconv.Atoi(match)
	if err != nil || number < 1 {
		return fallback
	}

	return number
}

func makeEpisodeID(ref EpisodeRef) string {
	data, _ := json.Marshal(ref)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeEpisodeID(id string) (EpisodeRef, error) {
	var ref EpisodeRef

	data, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		return ref, errors.New("invalid episode id")
	}

	if err := json.Unmarshal(data, &ref); err != nil {
		return ref, errors.New("invalid episode id")
	}

	if ref.TitleID == "" ||
		ref.RIP == "" ||
		ref.FID == "" ||
		ref.Disk == "" {
		return ref, errors.New("incomplete episode id")
	}

	return ref, nil
}

// --------------------------------------------------
// SEARCH
// --------------------------------------------------

func (c *Client) search(
	ctx context.Context,
	query string,
	page int,
) ([]map[string]any, error) {
	result, err := c.upstream(ctx, url.Values{
		"interface": {"search"},
		"q":         {query},
		"filter":    {"all"},
		"page":      {strconv.Itoa(page)},
	})
	if err != nil {
		return nil, err
	}

	items := rawArray(result["data"])
	if len(items) == 0 {
		items = rawArray(result["results"])
	}

	output := make([]map[string]any, 0, len(items))

	for _, raw := range items {
		item := objectValue(raw)

		id := firstString(item, "id")
		title := firstString(item, "title", "name")

		if id == "" || title == "" {
			continue
		}

		poster := firstString(item, "thumb", "poster", "image")
		if poster != "" &&
			!strings.HasPrefix(poster, "https://") {
			poster = posterBaseURL + strings.TrimLeft(poster, "/")
		}

		year := firstString(item, "year")

		output = append(output, map[string]any{
			"id":            id,
			"provider":      providerID,
			"title":         title,
			"media_type":    "series",
			"year":          year,
			"poster":        poster,
			"episode_count": nil,
			"season_count":  nil,
		})
	}

	return output, nil
}

// --------------------------------------------------
// DETAILS
// --------------------------------------------------

func (c *Client) details(
	ctx context.Context,
	id string,
) (map[string]json.RawMessage, error) {
	return c.upstream(ctx, url.Values{
		"interface": {"title_v2"},
		"id":        {id},
	})
}

func (c *Client) getSeasons(
	ctx context.Context,
	id string,
) ([]map[string]json.RawMessage, error) {
	details, err := c.details(ctx, id)
	if err != nil {
		return nil, err
	}

	seasons := rawArray(details["seasons"])
	if len(seasons) == 0 {
		seasons = rawArray(details["data"])
	}

	result := make([]map[string]json.RawMessage, 0, len(seasons))

	for _, raw := range seasons {
		item := objectValue(raw)

		rip := firstString(item, "rip")
		if rip == "" {
			continue
		}

		result = append(result, item)
	}

	return result, nil
}

// --------------------------------------------------
// EPISODES
// --------------------------------------------------

func (c *Client) getSeasonEpisodes(
	ctx context.Context,
	id string,
	rip string,
) ([]map[string]any, error) {
	result, err := c.upstream(ctx, url.Values{
		"interface": {"eplist"},
		"id":        {id},
		"season":    {rip},
	})
	if err != nil {
		return nil, err
	}

	items := rawArray(result["data"])
	if len(items) == 0 {
		items = rawArray(result["episodes"])
	}

	output := make([]map[string]any, 0, len(items))

	for index, raw := range items {
		item := objectValue(raw)

		fid := firstString(item, "fid")
		disk := firstString(item, "disk")

		if fid == "" || disk == "" {
			continue
		}

		title := firstString(item, "title", "f_title")
		fileTitle := firstString(item, "f_title")

		number := parseNumber(fileTitle, title)
		if number == 0 {
			number = index + 1
		}

		ref := EpisodeRef{
			TitleID: id,
			RIP:     rip,
			FID:     fid,
			Disk:    disk,
			Number:  number,
		}

		output = append(output, map[string]any{
			"id":          makeEpisodeID(ref),
			"number":      number,
			"season":      1,
			"title":       title,
			"description": nil,
			"quality":     firstString(item, "quality"),
			"size":        firstString(item, "size"),
		})
	}

	return output, nil
}

func (c *Client) episodes(
	ctx context.Context,
	id string,
) (map[string]any, error) {
	seasons, err := c.getSeasons(ctx, id)
	if err != nil {
		return nil, err
	}

	allEpisodes := make([]map[string]any, 0)
	seriesTitle := id

	details, detailErr := c.details(ctx, id)
	if detailErr == nil {
		seriesTitle = firstString(
			details,
			"title",
			"name",
		)
		if seriesTitle == "" {
			album := objectValue(details["album"])
			seriesTitle = firstString(album, "title", "name")
		}
	}
	if seriesTitle == "" {
		seriesTitle = id
	}

	for index, season := range seasons {
		rip := firstString(season, "rip")
		if rip == "" {
			continue
		}

		name := firstString(
			season,
			"season_name",
			"name",
			"version_name",
		)
		seasonNo := seasonNumber(name, index+1)

		seasonEpisodes, err := c.getSeasonEpisodes(
			ctx,
			id,
			rip,
		)
		if err != nil {
			log.Printf("episode list error for rip %s: %v", rip, err)
			continue
		}

		for _, episode := range seasonEpisodes {
			episode["season"] = seasonNo
			allEpisodes = append(allEpisodes, episode)
		}
	}

	if len(allEpisodes) == 0 {
		return nil, &ProviderError{
			Code:    "NOT_FOUND",
			Message: "No episodes found",
		}
	}

	return map[string]any{
		"provider":  providerID,
		"anime_id":  id,
		"title":     seriesTitle,
		"episodes":  allEpisodes,
	}, nil
}

// --------------------------------------------------
// SOURCE LOOKUP
// --------------------------------------------------

func (c *Client) sources(
	ctx context.Context,
	animeID string,
	episodeID string,
) (map[string]any, error) {
	ref, err := decodeEpisodeID(episodeID)
	if err != nil || ref.TitleID != animeID {
		return nil, &ProviderError{
			Code:    "INVALID_ID",
			Message: "Invalid episode ID",
		}
	}

	result, err := c.upstream(ctx, url.Values{
		"interface": {"getFile"},
		"fid":       {ref.FID},
		"findex":    {ref.Disk},
	})
	if err != nil {
		return nil, err
	}

	files := rawArray(result["fileInfo"])
	sources := make([]map[string]any, 0, len(files))

	for index, raw := range files {
		var file FileInfo
		if err := json.Unmarshal(raw, &file); err != nil {
			continue
		}

		parsed, err := url.Parse(file.URL)
		if err != nil ||
			parsed.Scheme != "https" ||
			parsed.Host == "" ||
			parsed.User != nil {
			continue
		}

		label := file.Host
		if label == "" {
			label = "Dramachi"
		}

		sourceID := fmt.Sprintf("%s-%d", ref.FID, index+1)

		sources = append(sources, map[string]any{
			"id":       sourceID,
			"label":    label,
			"type":     "file",
			"url":      file.URL,
			"quality":  nil,
			"language": nil,
			"headers":  map[string]string{},
			"direct_file": file.IsDL,
			"filename": file.Filename,
			"size":     file.Size,
		})
	}

	if len(sources) == 0 {
		return nil, &ProviderError{
			Code:      "SOURCE_UNAVAILABLE",
			Message:   "No valid source was returned by Dramachi",
			Retryable: true,
		}
	}

	return map[string]any{
		"provider":    providerID,
		"anime_id":    animeID,
		"episode_id":  episodeID,
		"source_id":   nil,
		"sources":     sources,
		"subtitles":   []any{},
	}, nil
}

// --------------------------------------------------
// RESPONSE HELPERS
// --------------------------------------------------

func success(
	provider string,
	action string,
	data any,
) fiber.Map {
	return fiber.Map{
		"status":   "success",
		"provider": provider,
		"action":   action,
		"data":     data,
	}
}

func failure(
	provider string,
	action string,
	code string,
	message string,
	retryable bool,
) fiber.Map {
	return fiber.Map{
		"status": "error",
		"error": fiber.Map{
			"code":      code,
			"message":   message,
			"provider":  provider,
			"action":    action,
			"retryable": retryable,
		},
	}
}

func sendError(
	c *fiber.Ctx,
	provider string,
	action string,
	err error,
) error {
	var providerErr *ProviderError

	if errors.As(err, &providerErr) {
		return c.JSON(failure(
			provider,
			action,
			providerErr.Code,
			providerErr.Message,
			providerErr.Retryable,
		))
	}

	log.Printf("%s %s error: %v", provider, action, err)

	code := "INTERNAL_ERROR"
	retryable := false

	if errors.Is(err, context.DeadlineExceeded) {
		code = "TIMEOUT"
		retryable = true
	}

	return c.JSON(failure(
		provider,
		action,
		code,
		"An internal provider error occurred.",
		retryable,
	))
}

// --------------------------------------------------
// MAIN ROUTES
// --------------------------------------------------

func main() {
	client := NewClient()

	app := fiber.New(fiber.Config{
		AppName:      "Dramachi Prophecy Sub Server",
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	})

	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"name":    "Dramachi Prophecy Sub Server",
			"status":  "online",
			"version": "1.0.0",
			"health":  "/health",
		})
	})

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":          "ok",
			"service":         "prophecy-dramachi",
			"provider_enabled": true,
		})
	})

	// SEARCH
	app.Get("/api/v1/anime/search", func(c *fiber.Ctx) error {
		query := strings.TrimSpace(c.Query("q"))
		page := c.QueryInt("page", 1)

		if query == "" || len(query) > 150 {
			return c.JSON(failure(
				providerID,
				"search",
				"INVALID_PARAMETERS",
				"Search query is required.",
				false,
			))
		}

		if page < 1 || page > 64 {
			return c.JSON(failure(
				providerID,
				"search",
				"INVALID_PAGE",
				"Invalid search page.",
				false,
			))
		}

		result, err := client.search(
			c.UserContext(),
			query,
			page,
		)
		if err != nil {
			return sendError(c, providerID, "search", err)
		}

		return c.JSON(success(providerID, "search", result))
	})

	// DETAILS
	app.Get("/api/v1/anime/:slug", func(c *fiber.Ctx) error {
		id := strings.TrimSpace(c.Params("slug"))

		if id == "" {
			return c.JSON(failure(
				providerID,
				"details",
				"INVALID_ID",
				"Anime ID is required.",
				false,
			))
		}

		details, err := client.details(c.UserContext(), id)
		if err != nil {
			return sendError(c, providerID, "details", err)
		}

		title := firstString(details, "title", "name")
		if title == "" {
			album := objectValue(details["album"])
			title = firstString(album, "title", "name")
		}
		if title == "" {
			title = id
		}

		seasonItems, err := client.getSeasons(c.UserContext(), id)
		if err != nil {
			return sendError(c, providerID, "details", err)
		}

		seasonOutput := make([]map[string]any, 0, len(seasonItems))

		for index, item := range seasonItems {
			rip := firstString(item, "rip")
			name := firstString(
				item,
				"season_name",
				"name",
				"version_name",
			)

			seasonOutput = append(seasonOutput, map[string]any{
				"number":             seasonNumber(name, index+1),
				"provider_season_id": rip,
				"name":               name,
				"episodes":           []any{},
			})
		}

		poster := firstString(details, "thumb", "poster")
		if poster != "" &&
			!strings.HasPrefix(poster, "https://") {
			poster = posterBaseURL + strings.TrimLeft(poster, "/")
		}

		data := map[string]any{
			"id":           id,
			"provider":     providerID,
			"title":        title,
			"media_type":   "series",
			"year":         firstString(details, "year"),
			"description":  firstString(details, "content", "description"),
			"poster":       poster,
			"genres":       []any{},
			"season_count": len(seasonOutput),
			"seasons":      seasonOutput,
		}

		return c.JSON(success(providerID, "details", data))
	})

	// EPISODES
	app.Get("/api/v1/anime/:slug/episodes", func(c *fiber.Ctx) error {
		id := strings.TrimSpace(c.Params("slug"))

		if id == "" {
			return c.JSON(failure(
				providerID,
				"episodes",
				"INVALID_ID",
				"Anime ID is required.",
				false,
			))
		}

		result, err := client.episodes(c.UserContext(), id)
		if err != nil {
			return sendError(c, providerID, "episodes", err)
		}

		return c.JSON(success(providerID, "episodes", result))
	})

	// SOURCES
	app.Get(
		"/api/v1/anime/:slug/episodes/:episode_id/sources",
		func(c *fiber.Ctx) error {
			id := strings.TrimSpace(c.Params("slug"))
			episodeID := strings.TrimSpace(c.Params("episode_id"))

			if id == "" || episodeID == "" {
				return c.JSON(failure(
					providerID,
					"sources",
					"INVALID_PARAMETERS",
					"Anime ID and episode ID are required.",
					false,
				))
			}

			result, err := client.sources(
				c.UserContext(),
				id,
				episodeID,
			)
			if err != nil {
				return sendError(c, providerID, "sources", err)
			}

			return c.JSON(success(providerID, "sources", result))
		},
	)

	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "10000"
	}

	log.Printf("Dramachi server starting on :%s", port)
	log.Fatal(app.Listen(":" + port))
}
