package auth

import (
	"context"
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/helios-commerce/helios/platform/id"
	"github.com/helios-commerce/helios/platform/tenant"
)

var ErrInvalidToken = errors.New("auth: invalid token")

type Claims struct {
	TenantID string `json:"tid"`
	UserID   string `json:"uid"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

type Tokens struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
	TokenType    string `json:"token_type"`
}

type Service struct {
	secret []byte
	issuer string
	access time.Duration
}

func New(secret, issuer string) *Service {
	return &Service{secret: []byte(secret), issuer: issuer, access: 15 * time.Minute}
}

func (s *Service) Issue(tid, uid, role string) (Tokens, error) {
	now := time.Now().UTC()
	claims := Claims{
		TenantID: tid,
		UserID:   uid,
		Role:     role,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			Subject:   uid,
			ID:        id.NewULID(),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.access)),
			NotBefore: jwt.NewNumericDate(now),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	access, err := tok.SignedString(s.secret)
	if err != nil {
		return Tokens{}, err
	}
	refresh := id.NewULID()
	return Tokens{
		AccessToken:  access,
		RefreshToken: refresh,
		ExpiresIn:    int64(s.access.Seconds()),
		TokenType:    "Bearer",
	}, nil
}

func (s *Service) Parse(token string) (Claims, error) {
	var claims Claims
	parsed, err := jwt.ParseWithClaims(token, &claims, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, ErrInvalidToken
		}
		return s.secret, nil
	})
	if err != nil || !parsed.Valid {
		return Claims{}, ErrInvalidToken
	}
	return claims, nil
}

func ContextFromClaims(ctx context.Context, c Claims) context.Context {
	return tenant.With(ctx, tenant.Identity{TenantID: c.TenantID, UserID: c.UserID, Role: c.Role})
}
