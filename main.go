package main

import (
    "archive/zip"
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
    
    err = db.Ping()
    if err != nil {
        log.Fatal("База не отвечает: ", err)
    }
    fmt.Println("Подключение к базе успешно!")
    
    createTable()
    
    http.HandleFunc("/api/v0/prices", handlePrices)
    
    fmt.Println("Сервер запущен!")
    log.Fatal(http.ListenAndServe(":8080", nil))
}

func createTable() {
    query := `
    CREATE TABLE IF NOT EXISTS prices (
        id VARCHAR(255) PRIMARY KEY,
        created_at DATE NOT NULL,
        name TEXT NOT NULL,
        category TEXT NOT NULL,
        price NUMERIC NOT NULL
    );`
    
    _, err := db.Exec(query)
    if err != nil {
        log.Fatal("Ошибка создания таблицы: ", err)
    }
    fmt.Println("Таблица prices готова")
}

func handlePrices(w http.ResponseWriter, r *http.Request) {
    fmt.Println("Запрос:", r.Method, r.URL.Path)
    
    if r.Method == "POST" {
        handlePost(w, r)
    } else if r.Method == "GET" {
        handleGet(w, r)
    } else {
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
    
    tempFile, err := os.CreateTemp("", "upload-*.zip")
    if err != nil {
        http.Error(w, "Ошибка создания временного файла", http.StatusInternalServerError)
        return
    }
    defer os.Remove(tempFile.Name())
    defer tempFile.Close()
    
    _, err = io.Copy(tempFile, file)
    if err != nil {
        http.Error(w, "Ошибка сохранения файла", http.StatusInternalServerError)
        return
    }
    
    zipReader, err := zip.OpenReader(tempFile.Name())
    if err != nil {
        http.Error(w, "Ошибка открытия zip: "+err.Error(), http.StatusBadRequest)
        return
    }
    defer zipReader.Close()
    
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
    
    totalItems := 0
    cats := make(map[string]bool)
    totalPrice := 0.0
    
    tx, err := db.Begin()
    if err != nil {
        http.Error(w, "Ошибка транзакции", http.StatusInternalServerError)
        return
    }
    
    stmt, err := tx.Prepare("INSERT INTO prices (id, created_at, name, category, price) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING")
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
        
        pid := record[0]
        name := record[1]
        category := record[2]
        priceStr := record[3]
        dateStr := record[4]
        
        price, err := strconv.ParseFloat(priceStr, 64)
        if err != nil {
            fmt.Println("Ошибка преобразования цены:", priceStr)
            continue
        }
        
        _, err = stmt.Exec(pid, dateStr, name, category, price)
        if err != nil {
            fmt.Println("Ошибка вставки в БД:", err)
        } else {
            totalItems++
            cats[category] = true
            totalPrice += price
        }
    }
    
    err = tx.Commit()
    if err != nil {
        http.Error(w, "Ошибка сохранения", http.StatusInternalServerError)
        return
    }
    
    fmt.Printf("Добавлено: %d, категорий: %d, сумма: %.2f\n", totalItems, len(cats), totalPrice)
    
    resp := PostResponse{
        TotalItems:      totalItems,
        TotalCategories: len(cats),
        TotalPrice:      totalPrice,
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(resp)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
    fmt.Println("Обработка GET")
    
    rows, err := db.Query("SELECT id, created_at, name, category, price FROM prices")
    if err != nil {
        http.Error(w, "Ошибка чтения из БД", http.StatusInternalServerError)
        return
    }
    defer rows.Close()
    
    tmpDir, err := os.MkdirTemp("", "export-*")
    if err != nil {
        http.Error(w, "Ошибка создания папки", http.StatusInternalServerError)
        return
    }
    defer os.RemoveAll(tmpDir)
    
    csvPath := tmpDir + "\\data.csv"
    csvFile, err := os.Create(csvPath)
    if err != nil {
        http.Error(w, "Ошибка создания CSV", http.StatusInternalServerError)
        return
    }
    
    wtr := csv.NewWriter(csvFile)
    cnt := 0
    
    for rows.Next() {
        var id, name, category string
        var dt time.Time
        var price float64
        
        err := rows.Scan(&id, &dt, &name, &category, &price)
        if err != nil {
            fmt.Println("Ошибка сканирования:", err)
            continue
        }
        
        dateStr := dt.Format("2006-01-02")
        priceStr := strconv.FormatFloat(price, 'f', -1, 64)
        
        rec := []string{id, name, category, priceStr, dateStr}
        wtr.Write(rec)
        cnt++
    }
    
    wtr.Flush()
    csvFile.Close()
    
    fmt.Printf("Записано %d записей\n", cnt)
    
    zipPath := tmpDir + "\\data.zip"
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
