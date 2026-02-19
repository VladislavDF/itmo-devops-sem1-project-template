package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

type PostResponse struct {
	TotalItems      int     `json:"total_items"`
	TotalCategories int     `json:"total_categories"`
	TotalPrice      float64 `json:"total_price"`
}

var db *sql.DB

func main() {
	fmt.Println("Запускаем сервер на порту 8080")

	connStr := "user=validator password=val1dat0r dbname=project-sem-1 sslmode=disable host=localhost port=5432"

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Ошибка подключения к базе: ", err)
	}
	// FIXME 1: Добавлено закрытие соединения с БД при завершении работы
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("База не отвечает: ", err)
	}
	fmt.Println("Подключение к базе успешно!")

	createTable()

	http.HandleFunc("/api/v0/prices", handlePrices)

	// NOTE: Указал порт
	fmt.Println("Сервер запущен на порту 8080!")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func createTable() {
	// FIXME 2: Исправлен SQL запрос создания таблицы (было два CREATE TABLE)
	query := `
    CREATE TABLE IF NOT EXISTS prices (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        category VARCHAR(255) NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        create_date TIMESTAMP NOT NULL
    );`

	_, err := db.Exec(query)
	if err != nil {
		log.Fatal("Ошибка создания таблицы: ", err)
	}
	fmt.Println("Таблица prices готова")
}

func handlePrices(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Запрос:", r.Method, r.URL.Path)

	// NOTE: Заменил на switch вместо if/else
	switch r.Method {
	case "POST":
		handlePost(w, r)
	case "GET":
		handleGet(w, r)
	default:
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
	}
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Обработка POST")

	err := r.ParseMultipartForm(10 << 20)
	if err != nil {
		http.Error(w, "Ошибка парсинга формы: "+err.Error(), http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Нет файла: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	// FIXME 3: Работа с ZIP в памяти, без создания временного файла на диске
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "Ошибка чтения файла", http.StatusInternalServerError)
		return
	}

	zipReader, err := zip.NewReader(bytes.NewReader(fileBytes), int64(len(fileBytes)))
	if err != nil {
		http.Error(w, "Ошибка открытия zip: "+err.Error(), http.StatusBadRequest)
		return
	}

	var csvFile *zip.File
	for _, f := range zipReader.File {
		if f.Name == "data.csv" || f.Name == "sample_data/data.csv" {
			csvFile = f
			break
		}
	}

	if csvFile == nil {
		http.Error(w, "Нет data.csv в архиве", http.StatusBadRequest)
		return
	}

	csvReader, err := csvFile.Open()
	if err != nil {
		http.Error(w, "Ошибка открытия csv", http.StatusInternalServerError)
		return
	}
	defer csvReader.Close()

	reader := csv.NewReader(csvReader)
	records, err := reader.ReadAll()
	if err != nil {
		http.Error(w, "Ошибка чтения CSV: "+err.Error(), http.StatusBadRequest)
		return
	}

	fmt.Printf("Прочитано %d строк\n", len(records))

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Ошибка транзакции", http.StatusInternalServerError)
		return
	}

	// FIXME 4: Убрано поле id из вставки (теперь оно автоинкрементное)
	stmt, err := tx.Prepare("INSERT INTO prices (name, category, price, create_date) VALUES ($1, $2, $3, $4)")
	if err != nil {
		http.Error(w, "Ошибка подготовки запроса", http.StatusInternalServerError)
		return
	}
	defer stmt.Close()

	for _, record := range records {
		if len(record) < 5 {
			fmt.Println("Пропускаем строку - мало полей")
			continue
		}

		if record[0] == "id" {
			fmt.Println("Пропускаем заголовок")
			continue
		}

		name := record[1]
		category := record[2]
		priceStr := record[3]
		dateStr := record[4]

		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			fmt.Println("Ошибка преобразования цены:", priceStr)
			continue
		}

		// FIXME 4: Вставка без id
		_, err = stmt.Exec(name, category, price, dateStr)
		if err != nil {
			fmt.Println("Ошибка вставки в БД:", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		http.Error(w, "Ошибка сохранения", http.StatusInternalServerError)
		return
	}

	// FIXME 5: Статистика считается после вставки по всей БД
	var totalItems int
	var totalPrice float64
	var totalCategories int

	err = db.QueryRow("SELECT COUNT(*), COALESCE(SUM(price), 0) FROM prices").Scan(&totalItems, &totalPrice)
	if err != nil {
		log.Printf("Ошибка подсчёта статистики: %v", err)
	}

	err = db.QueryRow("SELECT COUNT(DISTINCT category) FROM prices").Scan(&totalCategories)
	if err != nil {
		log.Printf("Ошибка подсчёта категорий: %v", err)
	}

	fmt.Printf("В БД всего: %d, категорий: %d, сумма: %.2f\n", totalItems, totalCategories, totalPrice)

	resp := PostResponse{
		TotalItems:      totalItems,
		TotalCategories: totalCategories,
		TotalPrice:      totalPrice,
	}

	w.Header().Set("Content-Type", "application/json")
	// NOTE: Добавлена обработка ошибки при отправке JSON
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		log.Printf("Ошибка отправки JSON: %v", err)
	}
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Обработка GET")

	rows, err := db.Query("SELECT id, create_date, name, category, price FROM prices ORDER BY id")
	if err != nil {
		http.Error(w, "Ошибка чтения из БД", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// FIXME 6: Сначала читаем все данные в память
	type Record struct {
		ID       string
		Name     string
		Category string
		Price    float64
		Date     time.Time
	}

	var records []Record

	for rows.Next() {
		var r Record
		err := rows.Scan(&r.ID, &r.Date, &r.Name, &r.Category, &r.Price)
		if err != nil {
			http.Error(w, "Ошибка сканирования", http.StatusInternalServerError)
			return // FIXME 6: Выходим при ошибке, а не продолжаем
		}
		records = append(records, r)
	}

	// Проверяем ошибки после итерации
	if err = rows.Err(); err != nil {
		http.Error(w, "Ошибка при чтении строк", http.StatusInternalServerError)
		return
	}

	tmpDir, err := os.MkdirTemp("", "export-*")
	if err != nil {
		http.Error(w, "Ошибка создания папки", http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(tmpDir)

	csvPath := tmpDir + "/data.csv" // FIXME: Использовать "/" вместо "\\" для кроссплатформенности
	csvFile, err := os.Create(csvPath)
	if err != nil {
		http.Error(w, "Ошибка создания CSV", http.StatusInternalServerError)
		return
	}

	wtr := csv.NewWriter(csvFile)

	for _, r := range records {
		rec := []string{
			r.ID,
			r.Name,
			r.Category,
			strconv.FormatFloat(r.Price, 'f', -1, 64),
			r.Date.Format("2006-01-02"),
		}
		if err := wtr.Write(rec); err != nil {
			http.Error(w, "Ошибка записи CSV", http.StatusInternalServerError)
			return
		}
	}

	wtr.Flush()
	if err := wtr.Error(); err != nil {
		http.Error(w, "Ошибка при записи CSV", http.StatusInternalServerError)
		return
	}
	csvFile.Close()

	fmt.Printf("Записано %d записей\n", len(records))

	zipPath := tmpDir + "/data.zip"
	zipFile, err := os.Create(zipPath)
	if err != nil {
		http.Error(w, "Ошибка создания ZIP", http.StatusInternalServerError)
		return
	}

	zw := zip.NewWriter(zipFile)

	f2, err := os.Open(csvPath)
	if err != nil {
		http.Error(w, "Ошибка открытия CSV", http.StatusInternalServerError)
		return
	}
	defer f2.Close()

	ze, err := zw.Create("data.csv")
	if err != nil {
		http.Error(w, "Ошибка создания в ZIP", http.StatusInternalServerError)
		return
	}

	_, err = io.Copy(ze, f2)
	if err != nil {
		http.Error(w, "Ошибка копирования в ZIP", http.StatusInternalServerError)
		return
	}

	zw.Close()
	zipFile.Close()

	out, err := os.Open(zipPath)
	if err != nil {
		http.Error(w, "Ошибка открытия ZIP", http.StatusInternalServerError)
		return
	}
	defer out.Close()

	st, err := out.Stat()
	if err != nil {
		http.Error(w, "Ошибка статистики", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", st.Size()))

	http.ServeContent(w, r, "data.zip", time.Now(), out)

	fmt.Println("ZIP отправлен")
}
