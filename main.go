package main

import (
	"context"
	"depthGo/database"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	SDK "github.com/CoinAPI/coinapi-sdk/data-api/go-rest/v1"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Parser struct {
	db      *mongo.Database
	Symbols []string
	Limit   uint32
	EndTime time.Time
	ApiKeys chan string
}

type Keys struct {
	Keys []string `json:"keys"`
}

type Bid struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

type Orderbook struct {
	Symbol_id     string    `json:"symbol_id"`
	Time_exchange time.Time `json:"time_exchange"`
	Time_coinapi  time.Time `json:"time_coinapi"`
	Asks          []Bid     `json:"asks"`
	Bids          []Bid     `json:"bids"`
}

func loadApiKeys(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var config Keys
	err = json.Unmarshal(bytes, &config)
	if err != nil {
		return nil, err
	}
	return config.Keys, nil
}

func InitParser(db *mongo.Database, Limit uint32, checkApiKeys bool) *Parser {
	dateNow, _ := time.Parse(time.RFC3339, os.Getenv("DataEnd"))
	ApiKeys, _ := loadApiKeys("apiKeys.json")

	workingApiKeys := []string{}

	if checkApiKeys {
		// Проверяем ключи перед добавлением в канал, если флаг checkApiKeys установлен в true
		for _, apiKey := range ApiKeys {
			// Создаем SDK клиент с ключом
			sdk := SDK.NewSDK(apiKey)

			// Пробуем сделать тестовый запрос с лимитом 100
			_, err := sdk.Orderbooks_historical_data_with_limit("BINANCE_SPOT_MKR_USDT", dateNow.Add(-24*time.Hour), 100)
			if err != nil {
				fmt.Printf("API ключ %s не работает: %v\n", apiKey, err)
			} else {
				fmt.Printf("API ключ %s работает корректно\n", apiKey)
				workingApiKeys = append(workingApiKeys, apiKey)
			}
		}

	} else {
		// Если проверка ключей не требуется, используем все ключи без проверки
		workingApiKeys = ApiKeys
	}

	// Создаем канал только с работающими ключами (если проверка производилась)
	apiKeyChan := make(chan string, len(workingApiKeys))
	for _, key := range workingApiKeys {
		apiKeyChan <- key
	}

	// Выводим общее количество работающих ключей
	fmt.Printf("Количество работающих API ключей: %d\n", len(workingApiKeys))

	p := Parser{
		db:      db,
		Symbols: strings.Split(os.Getenv("Symbols"), ","), // Список символов валют
		Limit:   Limit,
		EndTime: dateNow,
		ApiKeys: apiKeyChan, // Канал с работающими ключами
	}
	return &p
}

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	// Подключаемся к базе данных
	client, ctx, err := database.ConnectToDatabase()
	if err != nil {
		log.Fatal(err)
	}
	defer database.CloseDatabase(client, ctx)

	db := client.Database(os.Getenv("DbName"))

	p := InitParser(db, 100000, true)

	p.StartParser()

}

func (p *Parser) StartParser() {
	var wg sync.WaitGroup
	for i := 0; i < len(p.Symbols); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case apiKey, ok := <-p.ApiKeys:
					if !ok {
						// Если канал закрыт и ключей больше нет, завершаем горутину
						fmt.Printf("Горутина для символа %s завершена: нет доступных ключей\n", p.Symbols[i])
						return
					}

					err := ParseAndSave(p.db, p.Symbols[i], apiKey, p.EndTime, p.Limit)
					if err != nil {
						fmt.Printf("Ошибка для символа %s: %v\n", p.Symbols[i], err)
						// Логируем ошибку
						LogAPIError(apiKey, err)

						// Возвращаем API ключ в канал через 3 часа
						time.AfterFunc(3*time.Hour, func() {
							p.ApiKeys <- apiKey
						})

					} else {
						// Если нет ошибок, возвращаем ключ обратно в канал сразу
						p.ApiKeys <- apiKey
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("All workers finished")
}

// Завершение работы, когда все ключи исчерпаны
func CloseApiKeysChannel(p *Parser) {
	close(p.ApiKeys)
}

// ParseAndSave парсит данные и сохраняет их в MongoDB
func ParseAndSave(db *mongo.Database, symbol_id string, API_KEY string, dateNow time.Time, limit uint32) error {
	startTime, err := GetLatestTimeExchange(db, symbol_id)
	if err != nil {
		return err
	}
	fmt.Printf("startTime: %v, Валюта: %v \n", startTime, symbol_id)

	for {
		sdk := SDK.NewSDK(API_KEY)

		// Получаем исторические данные ордербуков
		Orderbooks_historical_data, err := sdk.Orderbooks_historical_data_with_limit(symbol_id, startTime, limit)

		if err != nil {
			fmt.Printf("Error fetching historical order book data for API_KEY: %s, Symbol: %s\n", API_KEY, symbol_id)
			// Возвращаем ошибку для обработки
			return err
		}

		fmt.Printf("Получено %d данных для символа %s\n", len(Orderbooks_historical_data), symbol_id)

		// Сохраняем данные в коллекцию с именем символа валюты
		SaveData(db, Orderbooks_historical_data, symbol_id)

		// Обновляем startTime для следующего запроса
		startTime = NextDate(Orderbooks_historical_data[len(Orderbooks_historical_data)-1])
		Orderbooks_historical_data = nil

		if !startTime.Before(dateNow) {
			break
		}
		time.Sleep(time.Second * 5)
	}

	return nil
}

// NextDate вычисляет следующую дату для запроса
func NextDate(data SDK.Orderbook) time.Time {
	return data.Time_exchange.Add(time.Second * 50)
}

// SaveData сохраняет данные в коллекцию, название которой соответствует символу валюты
func SaveData(db *mongo.Database, Orderbooks_historical_data []SDK.Orderbook, symbol_id string) {
	// Получаем коллекцию по имени символа валюты
	collection := db.Collection(symbol_id)

	// Преобразуем данные в []interface{} для вставки в MongoDB

	var documents []interface{}
	for _, orderbook := range Orderbooks_historical_data {
		doc := Orderbook{
			Symbol_id:     orderbook.Symbol_id,
			Time_exchange: orderbook.Time_exchange,
			Time_coinapi:  orderbook.Time_coinapi,
		}

		// Преобразуем Asks
		for _, ask := range orderbook.Asks {
			doc.Asks = append(doc.Asks, Bid{
				Price: ask.Price.String(), // Преобразуем decimal.Decimal в строку
				Size:  ask.Size.String(),  // Преобразуем decimal.Decimal в строку
			})
		}

		// Преобразуем Bids
		for _, bid := range orderbook.Bids {
			doc.Bids = append(doc.Bids, Bid{
				Price: bid.Price.String(), // Преобразуем decimal.Decimal в строку
				Size:  bid.Size.String(),  // Преобразуем decimal.Decimal в строку
			})
		}
		documents = append(documents, doc)

	}

	// Вставляем данные в коллекцию
	_, err := collection.InsertMany(context.TODO(), documents)

	if err != nil {
		fmt.Printf("Ошибка при вставке данных для символа %s: %v\n", symbol_id, err)
		return
	}

	fmt.Printf("Данные для символа %s успешно сохранены в MongoDB\n", symbol_id)
}

func GetLatestTimeExchange(db *mongo.Database, symbol_id string) (time.Time, error) {
	// Получаем коллекцию по имени символа
	collection := db.Collection(symbol_id)

	// Параметры поиска и сортировки
	opts := options.FindOne().SetSort(bson.D{{"time_exchange", -1}}) // Сортировка по убыванию даты

	// Структура для хранения результата
	var result struct {
		TimeExchange time.Time `bson:"time_exchange"`
	}

	// Выполняем запрос
	err := collection.FindOne(context.TODO(), bson.D{}, opts).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// Если документов нет, возвращаем заранее установленную дату
			defaultTime, _ := time.Parse(time.RFC3339, os.Getenv("DataStart"))
			return defaultTime, nil
		}
		return time.Time{}, fmt.Errorf("ошибка при получении последней даты time_exchange: %v", err)
	}

	// Возвращаем найденное время
	return result.TimeExchange, nil
}

// LogAPIError записывает ошибочные API ключи и ошибки в файл
func LogAPIError(apiKey string, err error) {
	// Открываем файл в режиме добавления (или создаём его, если он не существует)
	file, fileErr := os.OpenFile("api_errors.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if fileErr != nil {
		fmt.Printf("Ошибка при открытии файла для записи: %v\n", fileErr)
		return
	}
	defer file.Close()

	// Записываем API ключ и сообщение об ошибке
	_, writeErr := file.WriteString(fmt.Sprintf("API_KEY: %s, Error: %v\n", apiKey, err))
	if writeErr != nil {
		fmt.Printf("Ошибка при записи в файл: %v\n", writeErr)
	}
}
