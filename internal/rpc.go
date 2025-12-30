package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type BlockTrackerConfig struct {
	RPCURL            string
	MyBlsKey          string
	CheckBlockProof   bool
	CheckValidatorSet bool
	PollInterval      time.Duration
	Output            io.Writer
}

type BlockTracker struct {
	cfg           BlockTrackerConfig
	normalizedKey string
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type ValidatorSetInfo struct {
	BlsKey      string `json:"blsKey"`
	IdentityKey string `json:"identityKey"`
	Staking     string `json:"staking"`
	ValidatorID string `json:"validatorID"`
}

type ValidatorInfo struct {
	BlockNumber  string             `json:"blockNumber"`
	ValidatorSet []ValidatorSetInfo `json:"validatorSet"`
}

type BlockProof struct {
	BlockNumber            string   `json:"blockNumber"`
	BlockProofHash         string   `json:"blockProofHash"`
	BlsAggregatedSignature string   `json:"blsAggregatedSignature"`
	SignedBlsKeys          []string `json:"signedBlsKeys"`
}

func NewBlockTracker(cfg BlockTrackerConfig) (*BlockTracker, error) {
	if cfg.RPCURL == "" {
		return nil, fmt.Errorf("rpc url is required")
	}
	if cfg.CheckBlockProof && strings.TrimSpace(cfg.MyBlsKey) == "" {
		return nil, fmt.Errorf("my bls key is required when check block proof is enabled")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.Output == nil {
		cfg.Output = os.Stdout
	}

	m := &BlockTracker{
		cfg:           cfg,
		normalizedKey: normalizeBlsKey(cfg.MyBlsKey),
	}
	return m, nil
}

func (m *BlockTracker) Start(ctx context.Context) error {
	latestHex, err := fetchBlockNumber(m.cfg.RPCURL)
	if err != nil {
		return fmt.Errorf("fetch latest block number failed: %w", err)
	}
	lastChecked, _, err := parseHeight(latestHex)
	if err != nil {
		return fmt.Errorf("parse latest block number failed: %w", err)
	}
	if lastChecked > 0 {
		lastChecked--
	}
	fmt.Fprintf(m.cfg.Output, "RPC: %s start from height: %d\n", m.cfg.RPCURL, lastChecked+1)

	for {
		latestHex, err := fetchBlockNumber(m.cfg.RPCURL)
		if err != nil {
			return fmt.Errorf("fetch latest block number failed: %w", err)
		}
		latest, _, err := parseHeight(latestHex)
		if err != nil {
			return fmt.Errorf("parse latest block number failed: %w", err)
		}
		if latest <= lastChecked {
			if err := sleepWithContext(ctx, m.cfg.PollInterval); err != nil {
				return err
			}
			continue
		}

		for h := lastChecked + 1; h <= latest; h++ {
			heightHex := fmt.Sprintf("0x%x", h)

			if m.cfg.CheckBlockProof {
				bp, err := fetchBlockProof(m.cfg.RPCURL, heightHex)
				if err != nil {
					return fmt.Errorf("fetch block proof failed (height=%s): %w", heightHex, err)
				}
				found := false
				for _, pk := range bp.SignedBlsKeys {
					if normalizeBlsKey(pk) == m.normalizedKey {
						found = true
						break
					}
				}
				writeBoolMetric(m.cfg.Output, "validator_blockproof_has_bls_key", found)
				fmt.Fprintf(m.cfg.Output, "validator_blockproof_last_height %d\n", h)
			}

			if m.cfg.CheckValidatorSet {
				validators, err := fetchValidators(m.cfg.RPCURL, heightHex)
				if err != nil {
					return fmt.Errorf("fetch validators failed (height=%s): %w", heightHex, err)
				}
				found := false
				for _, v := range validators {
					if normalizeBlsKey(v.BlsKey) == m.normalizedKey {
						found = true
					}
				}
				writeBoolMetric(m.cfg.Output, "validator_validatorset_has_bls_key", found)
				fmt.Fprintf(m.cfg.Output, "validator_validatorset_last_height %d\n", h)
			}
		}
		lastChecked = latest

		if err := sleepWithContext(ctx, m.cfg.PollInterval); err != nil {
			return err
		}
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func writeBoolMetric(w io.Writer, name string, v bool) {
	if v {
		fmt.Fprintf(w, "%s 1\n", name)
		return
	}
	fmt.Fprintf(w, "%s 0\n", name)
}

func trim0x(s string) string {
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		return s[2:]
	}
	return s
}

func rpcPost(url, method string, params interface{}) (json.RawMessage, error) {
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}
	b, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("http post: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var r rpcResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("unmarshal rpc response: %w (body=%s)", err, string(body))
	}
	if r.Error != nil {
		return nil, fmt.Errorf("rpc error: %d %s", r.Error.Code, r.Error.Message)
	}
	return r.Result, nil
}

func fetchBlockNumber(rpcURL string) (string, error) {
	resultRaw, err := rpcPost(rpcURL, "eth_blockNumber", []interface{}{})
	if err != nil {
		return "0x0", fmt.Errorf("rpc call eth_blockNumber failed: %w", err)
	}

	var hexStr string
	if err := json.Unmarshal(resultRaw, &hexStr); err != nil {
		return "0x0", fmt.Errorf("parse eth_blockNumber result failed: %w", err)
	}

	return hexStr, nil
}

func fetchValidators(rpcURL string, height interface{}) ([]ValidatorSetInfo, error) {
	resultRaw, err := rpcPost(rpcURL, "debug_getValidatorInfo", []interface{}{height})
	if err != nil {
		return nil, err
	}

	var vInfo ValidatorInfo
	if err := json.Unmarshal(resultRaw, &vInfo); err != nil {
		return nil, err
	}

	return vInfo.ValidatorSet, nil
}

func fetchBlockProof(rpcURL string, height interface{}) (*BlockProof, error) {
	resultRaw, err := rpcPost(rpcURL, "debug_getBlockProof", []interface{}{height})
	if err != nil {
		return nil, err
	}
	var bp BlockProof
	if err := json.Unmarshal(resultRaw, &bp); err != nil {
		return nil, fmt.Errorf("parse block proof: %w", err)
	}
	return &bp, nil
}

func parseHeight(s string) (uint64, bool, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "latest" {
		return 0, true, nil
	}
	v, err := strconv.ParseUint(s, 0, 64)
	if err != nil {
		return 0, false, fmt.Errorf("invalid height %q: %w", s, err)
	}
	return v, false, nil
}

func normalizeBlsKey(s string) string {
	s = strings.ToLower(trim0x(strings.TrimSpace(s)))
	if len(s) > 96 && len(s)%2 == 0 {
		s = s[len(s)-96:]
	}
	return s
}
