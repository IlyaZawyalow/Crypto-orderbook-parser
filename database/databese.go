package database

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// ConnectToDatabase подключается к базе данных и возвращает клиент MongoDB
func ConnectToDatabase() (*mongo.Client, context.Context, error) {
	// Получение хоста и порта из переменных окружения
	host := os.Getenv("Host")
	port := os.Getenv("DbPort")

	if host == "" || port == "" {
		return nil, nil, fmt.Errorf("Host или DbPort не заданы в переменных окружения")
	}

	// Формирование строки подключения
	connStr := fmt.Sprintf("mongodb://%s:%s", host, port)

	// Установите контекст с таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)

	// Не вызываем defer cancel() здесь, контекст должен закрываться там, где используется
	clientOptions := options.Client().ApplyURI(connStr)

	// Подключение к MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		cancel() // Закрываем контекст, если подключение не удалось
		return nil, nil, fmt.Errorf("ошибка подключения к MongoDB: %v", err)
	}

	// Проверка подключения
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		cancel() // Закрываем контекст, если не удалось выполнить пинг
		return nil, nil, fmt.Errorf("не удалось подключиться к базе данных: %v", err)
	}

	fmt.Println("Успешно подключено к MongoDB")

	// Возвращаем клиент и контекст
	return client, ctx, nil
}

// CloseDatabase завершает соединение с MongoDB
func CloseDatabase(client *mongo.Client, ctx context.Context) {
	if err := client.Disconnect(ctx); err != nil {
		log.Printf("Ошибка при отключении от MongoDB: %v", err)
	} else {
		fmt.Println("Соединение с MongoDB успешно закрыто")
	}
}
