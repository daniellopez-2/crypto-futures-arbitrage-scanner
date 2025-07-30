package exchanges

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

type OKXTradeMessage struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		InstID    string `json:"instId"`
		TradeID   string `json:"tradeId"`
		Price     string `json:"px"`
		Size      string `json:"sz"`
		Side      string `json:"side"`
		Timestamp string `json:"ts"`
	} `json:"data"`
}

func ConnectOKXFutures(symbols []string, priceChan chan<- PriceData, tradeChan chan<- TradeData) {
	wsURL := "wss://ws.okx.com:8443/ws/v5/public"

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("OKX connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to OKX futures WebSocket")

		// Subscribe to trades for each symbol
		for _, symbol := range symbols {
			// Convert BTCUSDT to BTC-USDT-SWAP for OKX
			okxSymbol := convertToOKXSymbol(symbol)

			subscribeMsg := map[string]interface{}{
				"op": "subscribe",
				"args": []map[string]string{
					{
						"channel": "trades",
						"instId":  okxSymbol,
					},
				},
			}

			err = conn.WriteJSON(subscribeMsg)
			if err != nil {
				log.Printf("OKX subscription error for %s: %v", okxSymbol, err)
				continue
			}
			log.Printf("OKX subscribed to trades channel for: %s", okxSymbol)
		}

		for {
			var rawMessage map[string]interface{}
			err := conn.ReadJSON(&rawMessage)
			if err != nil {
				log.Printf("OKX read error: %v", err)
				conn.Close()
				break
			}

			// Check if it's a trade message
			if arg, ok := rawMessage["arg"].(map[string]interface{}); ok {
				if channel, exists := arg["channel"].(string); exists && channel == "trades" {
					var message OKXTradeMessage
					messageBytes, _ := json.Marshal(rawMessage)
					err = json.Unmarshal(messageBytes, &message)
					if err != nil {
						log.Printf("OKX trade unmarshal error: %v", err)
						continue
					}

					log.Printf("OKX processing %d trades for %s", len(message.Data), message.Arg.InstID)
					for _, trade := range message.Data {
						// Convert symbol back to standard format (BTC-USDT-SWAP -> BTCUSDT)
						symbol := convertFromOKXSymbol(trade.InstID)

						// Parse price and timestamp
						price, err := strconv.ParseFloat(trade.Price, 64)
						if err != nil {
							log.Printf("OKX price parse error: %v", err)
							continue
						}

						timestamp, err := strconv.ParseInt(trade.Timestamp, 10, 64)
						if err != nil {
							log.Printf("OKX timestamp parse error: %v", err)
							continue
						}

						// Normalize trade side (OKX uses "buy" and "sell")
						var side string
						if trade.Side == "buy" {
							side = "buy"
						} else {
							side = "sell"
						}

						priceData := PriceData{
							Symbol:    symbol,
							Exchange:  "okx_futures",
							Price:     price,
							Timestamp: timestamp,
						}

						tradeData := TradeData{
							Symbol:    symbol,
							Exchange:  "okx_futures",
							Price:     price,
							Quantity:  trade.Size,
							Side:      side,
							Timestamp: timestamp,
						}

						priceChan <- priceData
						tradeChan <- tradeData
					}
				}
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func convertToOKXSymbol(symbol string) string {
	// Convert BTCUSDT to BTC-USDT-SWAP (OKX's perpetual futures format)
	switch symbol {
	case "BTCUSDT":
		return "BTC-USDT-SWAP"
	case "ETHUSDT":
		return "ETH-USDT-SWAP"
	default:
		return symbol
	}
}

func convertFromOKXSymbol(symbol string) string {
	// Convert BTC-USDT-SWAP to BTCUSDT (standard format)
	switch symbol {
	case "BTC-USDT-SWAP":
		return "BTCUSDT"
	case "ETH-USDT-SWAP":
		return "ETHUSDT"
	default:
		return symbol
	}
}