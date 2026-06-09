package postman

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Postman выполняет операцию доставки писем в отдельной горутине.
// Функция прерывается при получении сигнала отмены из контекста.
func Postman(ctx context.Context, wg *sync.WaitGroup, transferPoint chan<- string, n int, mail string) {
	// Регистрация завершения работы горутины
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			// Получен сигнал отмены - прерываем работу
			fmt.Println("Я почтальон номер:", n, "Мой рабочий день закончен!")
			return
		default:
			// Выполнение операции доставки письма
			fmt.Println("Я почтальон номер:", n, "взял письмо:", mail)
			// Имитация времени обработки
			time.Sleep(1 * time.Second)
			fmt.Println("Я почтальон номер:", n, "отнёс письмо до почты:", mail)
			// Передача результата через канал
			transferPoint <- mail
			fmt.Println("Я почтальон номер:", n, "донес письмо:", mail)
		}
	}
}

// PostmanPool инициализирует пул горутин для параллельной доставки писем.
// Возвращает канал для приёма результатов из всех почтальонов.
func PostmanPool(ctx context.Context, postmanCount int) <-chan string {
	// Создание канала для передачи результатов доставки
	mailTransferPoint := make(chan string)

	// Инициализация группы ожидания для синхронизации горутин
	wg := &sync.WaitGroup{}

	// Запуск горутин для каждого почтальона
	for i := 1; i <= postmanCount; i++ {
		// Регистрация новой горутины в группе ожидания
		wg.Add(1)
		// Запуск горутины с индивидуальными параметрами
		go Postman(ctx, wg, mailTransferPoint, i, postmanToMail(i))
	}

	// Горутина для управления закрытием канала
	go func() {
		// Ожидание завершения всех рабочих горутин
		wg.Wait()
		// Закрытие канала после завершения всех операций
		close(mailTransferPoint)
	}()

	// Возврат канала для приёма данных
	return mailTransferPoint
}

// postmanToMail возвращает письмо для доставки на основе номера почтальона.
// Если номер не определён в таблице соответствия, возвращается стандартное письмо.
func postmanToMail(postmanNumber int) string {
	// Таблица соответствия номера почтальона к письму для доставки
	ptm := map[int]string{
		1: "Семейный привет",
		2: "Приглашение от друга",
		3: "Информация из сервиса",
	}

	// Поиск письма в таблице соответствия
	mail, ok := ptm[postmanNumber]
	// Если номер не найден в таблице
	if !ok {
		// Возврат стандартного письма
		return "Лотерея"
	}
	// Возврат найденного письма
	return mail
}
