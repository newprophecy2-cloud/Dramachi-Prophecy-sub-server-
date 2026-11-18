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

type ProviderError struct {
	Code string
	Message string
	Retryable bool
}
func (e *ProviderError) Error() string { return e.Message }

type cacheEntry struct { data []byte; expires time.Time }
type Client struct {
	httpClient *http.Client
	baseURL string
	retries int
	cacheTTL time.Duration
	mu sync.RWMutex
	cache map[string]cacheEntry
}

type Album struct {
	ID string `json:"id"`
	Title string `json:"title"`
	Content string `json:"content"`
	Year string `json:"year"`
	Storyline string `json:"storyline"`
	Director string `json:"director"`
	Genres string `json:"genres"`
	Thumb string `json:"thumb"`
}
type CastMember struct { Name string `json:"name"` }
type SeasonVersion struct {
	VersionName string `json:"version_name"`
	RIP string `json:"rip"`
	Server string `json:"server"`
}
type SeasonGroup struct {
	SeasonName string `json:"season_name"`
	Versions []SeasonVersion `json:"versions"`
}
type TitleResponse struct {
	Album []Album `json:"album"`
	Cast []CastMember `json:"cast"`
	Seasons map[string]SeasonGroup `json:"seasons"`
}
type SearchItem struct {
	ID string `json:"id"`
	Title string `json:"title"`
	Thumb string `json:"thumb"`
	Year string `json:"year"`
	Content string `json:"content"`
	Category string `json:"category"`
}
type SearchResponse struct { Data []SearchItem `json:"data"` }
type EpisodeItem struct {
	Title string `json:"title"`
	RIP string `json:"rip"`
	FTitle string `json:"f_title"`
	FID string `json:"fid"`
	Disk string `json:"disk"`
	Quality string `json:"quality"`
	Size string `json:"size"`
}
type EpisodeResponse struct { EpisodeList []EpisodeItem `json:"episode_list"` }
type FileItem struct {
	URL string `json:"url"`
	Filename string `json:"filename"`
	Title string `json:"title"`
	FTitle string `json:"f_title"`
	Size string `json:"size"`
}
type HostInfo struct {
	Host string `json:"host"`
	IsDL bool `json:"isDL"`
}
type FileResponse struct {
	FileInfo []FileItem `json:"fileInfo"`
	HostInfo HostInfo `json:"hostInfo"`
}
type EpisodeRef struct {
	TitleID string `json:"t"`
	RIP string `json:"r"`
	FID string `json:"f"`
	Disk string `json:"d"`
	Number int `json:"n"`
}
type SeasonRef struct {
	Number int
	Name string
	Version string
	RIP string
}

func envInt(key string, fallback int) int {
	n, err := strconv.Atoi(os.Getenv(key))
	if err != nil || n < 1 { return fallback }
	return n
}
func NewClient() *Client {
	base := strings.TrimSpace(os.Getenv("DRAMACHI_BASE_URL"))
	if base == "" { base = defaultBaseURL }
	return &Client{
		httpClient: &http.Client{Timeout: time.Duration(envInt("REQUEST_TIMEOUT", 20))*time.Second},
		baseURL: strings.TrimRight(base, "/") + "/",
		retries: envInt("REQUEST_RETRIES", 2),
		cacheTTL: time.Duration(envInt("CACHE_TTL", 60))*time.Second,
		cache: make(map[string]cacheEntry),
	}
}
func (c *Client) request(ctx context.Context, params url.Values) ([]byte, error) {
	endpoint := c.baseURL + "?" + params.Encode()
	sum := sha256.Sum256([]byte(endpoint))
	key := hex.EncodeToString(sum[:])
	c.mu.RLock()
	item, ok := c.cache[key]
	c.mu.RUnlock()
	if ok && time.Now().Before(item.expires) { return append([]byte(nil), item.data...), nil }

	var lastErr error
	for attempt := 0; attempt <= c.retries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil { return nil, err }
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "Prophecy-Dramachi-Server/1.0")
		resp, err := c.httpClient.Do(req)
		if err != nil { lastErr = err } else {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
			resp.Body.Close()
			if readErr != nil { lastErr = readErr
			} else if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				c.mu.Lock()
				c.cache[key] = cacheEntry{data: append([]byte(nil), body...), expires: time.Now().Add(c.cacheTTL)}
				c.mu.Unlock()
				return body, nil
			} else {
				lastErr = fmt.Errorf("upstream HTTP %d", resp.StatusCode)
				if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
					return nil, &ProviderError{Code:"UPSTREAM_HTTP_ERROR", Message:lastErr.Error()}
				}
			}
		}
		if attempt < c.retries {
			select {
			case <-ctx.Done(): return nil, ctx.Err()
			case <-time.After(time.Duration(attempt+1)*400*time.Millisecond):
			}
		}
	}
	return nil, &ProviderError{Code:"NETWORK_ERROR", Message:fmt.Sprintf("Dramachi request failed: %v", lastErr), Retryable:true}
}
func decode[T any](ctx context.Context, c *Client, params url.Values) (T, error) {
	var out T
	body, err := c.request(ctx, params)
	if err != nil { return out, err }
	if err := json.Unmarshal(body, &out); err != nil {
		return out, &ProviderError{Code:"PARSE_ERROR", Message:"Unexpected JSON structure from Dramachi"}
	}
	return out, nil
}
func (c *Client) title(ctx context.Context, id string) (TitleResponse, error) {
	return decode[TitleResponse](ctx, c, url.Values{"interface":{"title_v2"}, "id":{id}})
}
func (c *Client) seasonRefs(ctx context.Context, id string) ([]SeasonRef, error) {
	d, err := c.title(ctx, id)
	if err != nil { return nil, err }
	keys := make([]string, 0, len(d.Seasons))
	for key := range d.Seasons { keys = append(keys, key) }
	sortSeasonKeys(keys)
	out := make([]SeasonRef, 0)
	for _, key := range keys {
		group := d.Seasons[key]
		name := group.SeasonName
		if name == "" { name = key }
		num := seasonNumber(name, seasonNumber(key, 1))
		for _, version := range group.Versions {
			if strings.TrimSpace(version.RIP) == "" { continue }
			out = append(out, SeasonRef{Number:num, Name:name, Version:version.VersionName, RIP:version.RIP})
		}
	}
	return out, nil
}
func sortSeasonKeys(keys []string) {
	re := regexp.MustCompile(`\d+`)
	sort.Slice(keys, func(i,j int) bool {
		a,_ := strconv.Atoi(re.FindString(keys[i])); b,_ := strconv.Atoi(re.FindString(keys[j]))
		if a == b { return keys[i] < keys[j] }; return a < b
	})
}
func seasonNumber(s string, fallback int) int {
	m := regexp.MustCompile(`\d+`).FindString(s)
	if m == "" { return fallback }
	n, err := strconv.Atoi(m); if err != nil || n < 1 { return fallback }; return n
}
func episodeNumber(s string, fallback int) int {
	re := regexp.MustCompile(`(?i)(?:episode|ep\.?|e)[\s._-]*(\d+)`)
	m := re.FindStringSubmatch(s)
	if len(m) > 1 { n,_ := strconv.Atoi(m[1]); if n > 0 { return n } }
	fields := strings.Fields(s)
	if len(fields)>0 { n,err:=strconv.Atoi(fields[len(fields)-1]); if err==nil && n>0 { return n } }
	return fallback
}
func episodeID(ref EpisodeRef) string {
	b,_ := json.Marshal(ref)
	return base64.RawURLEncoding.EncodeToString(b)
}
func parseEpisodeID(id string) (EpisodeRef,error) {
	var ref EpisodeRef
	b,err:=base64.RawURLEncoding.DecodeString(id); if err!=nil { return ref,err }
	err=json.Unmarshal(b,&ref)
	if err!=nil || ref.TitleID=="" || ref.RIP=="" || ref.FID=="" || ref.Disk=="" { return ref,errors.New("invalid episode id") }
	return ref,nil
}
func posterURL(path string) string {
	path = strings.TrimSpace(path)
	if path == "" { return "" }
	u,err:=url.Parse(path)
	if err==nil && u.IsAbs() && u.Scheme=="https" { return path }
	return posterBaseURL + strings.TrimLeft(path, "/")
}
func (c *Client) search(ctx context.Context, q string, page int) ([]map[string]any,error) {
	d,err:=decode[SearchResponse](ctx,c,url.Values{"interface":{"search"},"q":{q},"filter":{"all"},"page":{strconv.Itoa(page)}})
	if err!=nil { return nil,err }
	out:=make([]map[string]any,0,len(d.Data))
	for _,v:=range d.Data {
		if v.ID=="" || v.Title=="" { continue }
		content:=strings.ToLower(v.Content+" "+v.Category)
		kind:="series"
		if strings.Contains(content,"movie") { kind="movie" }
		out=append(out,map[string]any{"id":v.ID,"provider":providerID,"title":v.Title,"media_type":kind,"year":v.Year,"poster":posterURL(v.Thumb),"episode_count":nil,"season_count":nil})
	}
	return out,nil
}
func (c *Client) episodesForSeason(ctx context.Context, id string, season SeasonRef) ([]map[string]any,error) {
	d,err:=decode[EpisodeResponse](ctx,c,url.Values{"interface":{"eplist"},"season":{season.RIP},"id":{id}})
	if err!=nil { return nil,err }
	out:=make([]map[string]any,0,len(d.EpisodeList))
	for i,ep:=range d.EpisodeList {
		if ep.FID=="" || ep.Disk=="" { continue }
		title:=ep.Title; if title=="" { title=ep.FTitle }
		n:=episodeNumber(ep.FTitle,i+1)
		ref:=EpisodeRef{TitleID:id,RIP:season.RIP,FID:ep.FID,Disk:ep.Disk,Number:n}
		out=append(out,map[string]any{"id":episodeID(ref),"number":n,"season":season.Number,"title":title,"description":nil,"quality":ep.Quality,"size":ep.Size,"version":season.Version})
	}
	return out,nil
}
func (c *Client) getEpisodes(ctx context.Context,id string) (map[string]any,error) {
	d,err:=c.title(ctx,id); if err!=nil { return nil,err }
	if len(d.Album)==0 { return nil,&ProviderError{Code:"NOT_FOUND",Message:"Dramachi title details contain no album"} }
	seasons,err:=c.seasonRefs(ctx,id); if err!=nil { return nil,err }
	all:=make([]map[string]any,0)
	for _,s:=range seasons {
		eps,e:=c.episodesForSeason(ctx,id,s)
		if e!=nil { log.Printf("Dramachi episode list (%s): %v",s.RIP,e); continue }
		all=append(all,eps...)
	}
	if len(all)==0 { return nil,&ProviderError{Code:"NOT_FOUND",Message:"No episodes found"} }
	return map[string]any{"provider":providerID,"anime_id":id,"title":d.Album[0].Title,"episodes":all},nil
}
func (c *Client) getSources(ctx context.Context, animeID, episodeID string) (map[string]any,error) {
	ref,err:=parseEpisodeID(episodeID)
	if err!=nil || ref.TitleID!=animeID { return nil,&ProviderError{Code:"INVALID_ID",Message:"Invalid episode ID"} }
	d,err:=decode[FileResponse](ctx,c,url.Values{"interface":{"getFile"},"fid":{ref.FID},"findex":{ref.Disk}})
	if err!=nil { return nil,err }
	sources:=make([]map[string]any,0,len(d.FileInfo))
	for i,f:=range d.FileInfo {
		raw:=strings.TrimSpace(f.URL); if raw=="" { continue }
		var finalURL string
		u,e:=url.Parse(raw)
		if e==nil && u.IsAbs() {
			if u.Scheme!="https" || u.Host=="" || u.User!=nil { continue }
			finalURL=u.String()
		} else {
			host:=strings.TrimSpace(d.HostInfo.Host)
			host=strings.TrimPrefix(strings.TrimPrefix(host,"https://"),"http://")
			host=strings.TrimRight(host,"/")
			if host=="" || strings.ContainsAny(host,"/@?#") { continue }
			finalURL="https://"+host+"/cdn/"+strings.TrimLeft(raw,"/")
		}
		filename:=f.Filename
		if filename=="" { filename=f.FTitle }
		if filename=="" { filename=f.Title }
		sources=append(sources,map[string]any{"id":fmt.Sprintf("%s-%d",ref.FID,i+1),"label":"Dramachi CDN","type":"file","url":finalURL,"quality":nil,"language":nil,"headers":map[string]string{},"direct_file":d.HostInfo.IsDL,"filename":filename,"size":f.Size})
	}
	if len(sources)==0 { return nil,&ProviderError{Code:"SOURCE_UNAVAILABLE",Message:"No source URL returned by Dramachi",Retryable:true} }
	return map[string]any{"provider":providerID,"anime_id":animeID,"episode_id":episodeID,"sources":sources,"subtitles":[]any{}},nil
}
func success(action string,data any) fiber.Map {
	return fiber.Map{"status":"success","provider":providerID,"action":action,"data":data}
}
func failure(action string,err error) fiber.Map {
	var pe *ProviderError
	if errors.As(err,&pe) { return fiber.Map{"status":"error","error":fiber.Map{"code":pe.Code,"message":pe.Message,"provider":providerID,"action":action,"retryable":pe.Retryable}} }
	log.Printf("Dramachi %s: %v",action,err)
	return fiber.Map{"status":"error","error":fiber.Map{"code":"INTERNAL_ERROR","message":"An internal provider error occurred.","provider":providerID,"action":action,"retryable":false}}
}
func main() {
	client:=NewClient()
	app:=fiber.New(fiber.Config{AppName:"Dramachi Prophecy Sub Server",ReadTimeout:30*time.Second,WriteTimeout:30*time.Second})
	app.Get("/",func(c *fiber.Ctx)error{return c.JSON(fiber.Map{"name":"Dramachi Prophecy Sub Server","status":"online","version":"1.0.1","health":"/health"})})
	app.Get("/health",func(c *fiber.Ctx)error{return c.JSON(fiber.Map{"status":"ok","service":"prophecy-dramachi","provider_enabled":true})})
	app.Get("/api/v1/anime/search",func(c *fiber.Ctx)error{
		q:=strings.TrimSpace(c.Query("q")); page:=c.QueryInt("page",1)
		if q=="" || len(q)>150 { return c.JSON(fiber.Map{"status":"error","error":fiber.Map{"code":"INVALID_PARAMETERS","message":"Search query is required."}}) }
		if page<1 || page>64 { page=1 }
		data,err:=client.search(c.UserContext(),q,page); if err!=nil{return c.JSON(failure("search",err))}
		return c.JSON(success("search",data))
	})
	app.Get("/api/v1/anime/:slug",func(c *fiber.Ctx)error{
		id:=strings.TrimSpace(c.Params("slug")); d,err:=client.title(c.UserContext(),id); if err!=nil{return c.JSON(failure("details",err))}
		if len(d.Album)==0 { return c.JSON(failure("details",&ProviderError{Code:"NOT_FOUND",Message:"Dramachi returned an empty album for this ID"})) }
		a:=d.Album[0]; refs,err:=client.seasonRefs(c.UserContext(),id); if err!=nil{return c.JSON(failure("details",err))}
		seasonMap:=map[int]map[string]any{}
		for _,s:=range refs {
			entry,ok:=seasonMap[s.Number]; if !ok { entry=map[string]any{"number":s.Number,"name":s.Name,"provider_season_id":s.RIP,"versions":[]any{},"episodes":[]any{}} }
			versions:=entry["versions"].([]any); versions=append(versions,map[string]any{"name":s.Version,"rip":s.RIP}); entry["versions"]=versions
			seasonMap[s.Number]=entry
		}
		seasonNums:=make([]int,0,len(seasonMap)); for n:=range seasonMap {seasonNums=append(seasonNums,n)}
		sort.Ints(seasonNums)
		seasons:=make([]map[string]any,0,len(seasonNums)); for _,n:=range seasonNums {seasons=append(seasons,seasonMap[n])}
		genres:=[]string{}; for _,g:=range strings.Split(a.Genres,","){if g=strings.TrimSpace(g);g!=""{genres=append(genres,g)}}
		kind:="series"; if strings.Contains(strings.ToLower(a.Content),"movie"){kind="movie"}
		data:=map[string]any{"id":id,"provider":providerID,"title":a.Title,"media_type":kind,"year":a.Year,"description":a.Storyline,"poster":posterURL(a.Thumb),"genres":genres,"director":a.Director,"cast":d.Cast,"season_count":len(seasons),"seasons":seasons}
		return c.JSON(success("details",data))
	})
	app.Get("/api/v1/anime/:slug/episodes",func(c *fiber.Ctx)error{
		data,err:=client.getEpisodes(c.UserContext(),strings.TrimSpace(c.Params("slug"))); if err!=nil{return c.JSON(failure("episodes",err))}
		return c.JSON(success("episodes",data))
	})
	app.Get("/api/v1/anime/:slug/episodes/:episode_id/sources",func(c *fiber.Ctx)error{
		data,err:=client.getSources(c.UserContext(),strings.TrimSpace(c.Params("slug")),strings.TrimSpace(c.Params("episode_id"))); if err!=nil{return c.JSON(failure("sources",err))}
		return c.JSON(success("sources",data))
	})
	port:=strings.TrimSpace(os.Getenv("PORT")); if port==""{port="10000"}
	log.Printf("Dramachi server listening on :%s",port)
	log.Fatal(app.Listen(":"+port))
}
