package search

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/backfill"
	"github.com/bluesky-social/indigo/util/version"
	"github.com/bluesky-social/indigo/xrpc"

	lru "github.com/hashicorp/golang-lru"
	flatfs "github.com/ipfs/go-ds-flatfs"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	es "github.com/opensearch-project/opensearch-go/v2"
	slogecho "github.com/samber/slog-echo"
	gorm "gorm.io/gorm"
)

type Server struct {
	escli        *es.Client
	postIndex    string
	profileIndex string
	db           *gorm.DB
	bgshost      string
	bgsxrpc      *xrpc.Client
	dir          identity.Directory
	echo         *echo.Echo
	logger       *slog.Logger

	bfs *backfill.Gormstore
	bf  *backfill.Backfiller

	userCache *lru.Cache
}

type LastSeq struct {
	ID  uint `gorm:"primarykey"`
	Seq int64
}

type Config struct {
	BGSHost      string
	ProfileIndex string
	PostIndex    string
	Logger       *slog.Logger
}

func NewServer(db *gorm.DB, escli *es.Client, dir identity.Directory, config Config) (*Server, error) {

	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}

	logger.Info("running database migrations")
	db.AutoMigrate(&LastSeq{})
	db.AutoMigrate(&backfill.GormDBJob{})

	bgsws := config.BGSHost
	if !strings.HasPrefix(bgsws, "ws") {
		return nil, fmt.Errorf("specified bgs host must include 'ws://' or 'wss://'")
	}

	bgshttp := strings.Replace(bgsws, "ws", "http", 1)
	bgsxrpc := &xrpc.Client{
		Host: bgshttp,
	}

	ucache, _ := lru.New(100000)
	s := &Server{
		escli:        escli,
		profileIndex: config.ProfileIndex,
		postIndex:    config.PostIndex,
		db:           db,
		bgshost:      config.BGSHost, // NOTE: the original URL, not 'bgshttp'
		bgsxrpc:      bgsxrpc,
		dir:          dir,
		userCache:    ucache,
		logger:       logger,
	}

	bfstore := backfill.NewGormstore(db)
	opts := backfill.DefaultBackfillOptions()
	bf := backfill.NewBackfiller(
		"search",
		bfstore,
		s.handleCreateOrUpdate,
		s.handleCreateOrUpdate,
		s.handleDelete,
		opts,
	)

	s.bfs = bfstore
	s.bf = bf

	return s, nil
}

func (s *Server) SearchPosts(ctx context.Context, q string, offset, size int) ([]PostSearchResult, error) {
	resp, err := DoSearchPosts(ctx, s.dir, s.escli, s.postIndex, q, offset, size)
	if err != nil {
		return nil, err
	}

	out := []PostSearchResult{}
	for _, r := range resp.Hits.Hits {
		if err != nil {
			return nil, fmt.Errorf("decoding document id: %w", err)
		}

		var doc PostDoc
		if err := json.Unmarshal(r.Source, &doc); err != nil {
			return nil, err
		}

		did, err := syntax.ParseDID(doc.DID)
		if err != nil {
			s.logger.Warn("invalid DID in indexed document", "did", doc.DID, "err", err)
			continue
		}
		handle := ""
		ident, err := s.dir.LookupDID(ctx, did)
		if err != nil {
			s.logger.Warn("could not resolve identity", "did", doc.DID)
			continue
		} else {
			handle = ident.Handle.String()
		}

		out = append(out, PostSearchResult{
			Tid: doc.RecordRkey,
			Cid: doc.RecordCID,
			User: UserResult{
				Did:    doc.DID,
				Handle: handle,
			},
			Post: &doc,
		})
	}

	return out, nil
}

var ErrDoneIterating = fmt.Errorf("done iterating")

func (s *Server) SearchProfiles(ctx context.Context, q string, typeahead bool, offset, size int) ([]*ActorSearchResp, error) {
	var resp *EsSearchResponse
	var err error
	if typeahead {
		resp, err = DoSearchProfilesTypeahead(ctx, s.escli, s.profileIndex, q)
	} else {
		resp, err = DoSearchProfiles(ctx, s.dir, s.escli, s.profileIndex, q, offset, size)
	}
	if err != nil {
		return nil, err
	}

	out := []*ActorSearchResp{}
	for _, r := range resp.Hits.Hits {
		var doc ProfileDoc
		if err := json.Unmarshal(r.Source, &doc); err != nil {
			return nil, err
		}

		out = append(out, &ActorSearchResp{
			ActorProfile: doc,
			DID:          doc.DID,
		})
	}

	return out, nil
}

func OpenBlockstore(dir string) (blockstore.Blockstore, error) {
	fds, err := flatfs.CreateOrOpen(dir, flatfs.IPFS_DEF_SHARD, false)
	if err != nil {
		return nil, err
	}

	return blockstore.NewBlockstoreNoPrefix(fds), nil
}

type HealthStatus struct {
	Status  string `json:"status"`
	Version string `json:"version"`
	Message string `json:"msg,omitempty"`
}

func (s *Server) handleHealthCheck(c echo.Context) error {
	if err := s.db.Exec("SELECT 1").Error; err != nil {
		s.logger.Error("healthcheck can't connect to database", "err", err)
		return c.JSON(500, HealthStatus{Status: "error", Version: version.Version, Message: "can't connect to database"})
	} else {
		return c.JSON(200, HealthStatus{Status: "ok", Version: version.Version})
	}
}

func (s *Server) RunAPI(listen string) error {

	s.logger.Info("Configuring HTTP server")
	e := echo.New()
	e.HideBanner = true
	e.Use(slogecho.New(s.logger))
	e.Use(middleware.Recover())
	e.Use(echoprometheus.NewMiddleware("palomar"))
	e.Use(middleware.BodyLimit("64M"))

	e.HTTPErrorHandler = func(err error, ctx echo.Context) {
		code := 500
		if he, ok := err.(*echo.HTTPError); ok {
			code = he.Code
		}
		s.logger.Warn("HTTP request error", "statusCode", code, "path", ctx.Path(), "err", err)
		ctx.Response().WriteHeader(code)
	}

	e.Use(middleware.CORS())
	e.GET("/_health", s.handleHealthCheck)
	e.GET("/metrics", echoprometheus.NewHandler())
	e.GET("/search/posts", s.handleSearchRequestPosts)
	e.GET("/search/profiles", s.handleSearchRequestProfiles)
	s.echo = e

	s.logger.Info("starting search API daemon", "bind", listen)
	return s.echo.Start(listen)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.echo.Shutdown(ctx)
}
