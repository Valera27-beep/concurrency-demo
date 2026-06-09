package miner

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Miner выполняет операцию добычи угля в отдельной горутине.
// Функция прерывается при получении сигнала отмены из контекста.
func Miner(ctx context.Context, wg *sync.WaitGroup, transferPoint chan<- int, n int, power int) {
	// Регистрация завершения работы горутины
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			// Получен сигнал отмены - прерываем работу
			fmt.Println("Я шахтёр номер:", n, "Мой рабочий день закончен!")
			return
		default:
			// Выполнение операции добычи угля
			fmt.Println("Я шахтёр номер:", n, "Начал добывать уголь!")
			// Имитация времени обработки
			time.Sleep(1 * time.Second)
			fmt.Println("Я шахтёр номер:", n, "Добыл уголь:", power)

			// Передача результата через канал
			transferPoint <- power
			fmt.Println("Я шахтёр номер:", n, "Передал уголь:", power)
		}
	}
}

// MinerPool инициализирует пул горутин для параллельной добычи угля.
// Возвращает канал для приёма результатов из всех шахтёров.
func MinerPool(ctx context.Context, minerCount int) <-chan int {
	// Создание канала для передачи результатов добычи
	coalTransferPoint := make(chan int)

	// Инициализация группы ожидания для синхронизации горутин
	wg := &sync.WaitGroup{}

	// Запуск горутин для каждого шахтёра
	for i := 1; i <= minerCount; i++ {
		// Регистрация новой горутины в группе ожидания
		wg.Add(1)
		// Запуск горутины с индивидуальными параметрами
		go Miner(ctx, wg, coalTransferPoint, i, i*10)
	}

	// Горутина для управления закрытием канала
	go func() {
		// Ожидание завершения всех рабочих горутин
		wg.Wait()
		// Закрытие канала после завершения всех операций
		close(coalTransferPoint)
	}()

	// Возврат канала для приёма данных
	return coalTransferPoint
}