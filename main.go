package main

import (
	"concurency/miner"
	"concurency/postman"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	// Инициализация времени начала выполнения
	startTime := time.Now()

	// Атомарный счётчик для хранения общего количества добытого угля
	var coal atomic.Int64
	// Слайс для хранения доставленных писем
	var mails []string
	// Мьютекс для синхронизации доступа к слайсу писем
	mtx := sync.Mutex{}

	// Создание контекста для управления жизненным циклом горутин шахтёров
	minerContext, minerCancel := context.WithCancel(context.Background())
	// Создание контекста для управления жизненным циклом горутин почтальонов
	postmanContext, postmanCancel := context.WithCancel(context.Background())

	// Запуск таймера отмены для шахтёров (3 секунды)
	go func() {
		time.Sleep(3 * time.Second)
		minerCancel()
	}()

	// Запуск таймера отмены для почтальонов (6 секунд)
	go func() {
		time.Sleep(6 * time.Second)
		postmanCancel()
	}()

	// Инициализация пула шахтёров с количеством горутин = 2
	coalTransferPoint := miner.MinerPool(minerContext, 2)
	// Инициализация пула почтальонов с количеством горутин = 2
	mailTransferPoint := postman.PostmanPool(postmanContext, 2)

	// Группа ожидания для синхронизации завершения всех горутин
	wg := &sync.WaitGroup{}

	// Горутина для агрегации результатов добычи угля
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range coalTransferPoint {
			// Добавление значения к атомарному счётчику угля
			coal.Add(int64(v))
		}
	}()

	// Горутина для агрегации результатов доставки писем
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range mailTransferPoint {
			// Блокировка доступа к слайсу писем
			mtx.Lock()
			// Добавление письма в слайс
			mails = append(mails, v)
			// Разблокировка доступа к слайсу писем
			mtx.Unlock()
		}
	}()

	// Ожидание завершения всех горутин
	wg.Wait()

	// Вывод общего количества добытого угля
	fmt.Printf("Суммарно добытый уголь: %d\n", coal.Load())
	// Вывод общего количества доставленных писем
	fmt.Printf("Суммарно получено писем: %d\n", len(mails))

	// Вычисление и вывод времени выполнения программы
	elapsed := time.Since(startTime)
	fmt.Printf("Время выполнения: %v\n", elapsed)
}