## O projekcie

Ten system służy do budowania **statycznej bazy danych połączeń lotniczych** w Europie. Zamiast polegać na drogich, dynamicznych zapytaniach w czasie rzeczywistym ("wyszukaj mi lot teraz"), budujemy własny "rozkład jazdy" (cache), skanując dostępność lotów na rok do przodu.

**Główne cele:**
1.  Zbudowanie mapy "Kto, skąd, dokąd i czym lata?" (Static Data).
2.  Analiza częstotliwości lotów (Dni operowania).
3.  Analiza kosztów i jakości (Cena vs Komfort vs Eco).
4.  Wizualizacja danych na interaktywnym dashboardzie.

---

## Architektura i Strategia

1.  **Input:** Lista kodów miast (IATA City Codes, np. `LON`, `PAR`) z pliku `europe_cities.json`.
2.  **Generator:** Tworzymy permutacje par miast (np. `WAW -> LIS`).
3.  **Skaner Tygodniowy:** Dla każdej pary sprawdzamy dostępność lotów przez **pełny tydzień** (Pn-Nd) w szczycie sezonu wakacyjnego (Lipiec 2026).
4.  **Baza Danych:** Wyniki są zapisywane w SQLite z podziałem na dane stałe (trasa, samolot) i zmienne (cena).
5.  **Dashboard:** Streamlit wizualizuje dane bezpośrednio z SQL.

---

## Struktura Projektu

```text
Duffel_api/
│
├── .env                    # Plik konfiguracyjny (API Token - NIE COMMITOWAĆ!)
├── README.MD               # Dokumentacja projektu
│
├── run_scanner.py          # GŁÓWNY SKRYPT: Uruchamia proces pobierania danych
├── dashboard.py            # FRONTEND: Aplikacja Streamlit (Analytics & Maps)
│
├── flights.db              # BAZA DANYCH: Plik SQLite (generowany automatycznie)
├── scanner.log             # LOGI: Plik z logami skanera (generowany automatycznie)
│
├── data/
│   └── europe_cities.json  # Lista wejściowa kodów IATA (Seed list)
│
└── core/                   # Logika biznesowa (Backend)
    ├── __init__.py
    ├── api.py              # Komunikacja z Duffel API (Requests & Error Handling)
    ├── config.py           # Ładowanie zmiennych środowiskowych
    ├── database.py         # Obsługa SQLite (Create Tables, Upsert, Logging)
    ├── parser.py           # Transformacja JSON z API na płaskie struktury SQL
    └── utils.py            # Funkcje pomocnicze
```

---

## Wymagania Systemowe

### Wersja Pythona
- **Python 3.10+** (wymagane przez Poetry i zależności)

### Zależności
Projekt używa następujących bibliotek (zarządzane przez Poetry):

| Biblioteka | Wersja | Opis |
|------------|--------|------|
| `requests` | >=2.32.5 | Komunikacja HTTP z API Duffel |
| `python-dotenv` | >=1.2.1 | Ładowanie zmiennych środowiskowych z pliku `.env` |
| `pandas` | >=2.3.3 | Przetwarzanie i analiza danych |
| `streamlit` | >=1.52.2 | Framework do budowania dashboardu |
| `plotly` | >=6.5.0 | Interaktywne wykresy i mapy |

---

## Instalacja i Konfiguracja

### 1. Instalacja Poetry (jeśli nie masz)

Poetry to narzędzie do zarządzania zależnościami Pythona. Zainstaluj je globalnie:

```bash
# Linux / macOS
curl -sSL https://install.python-poetry.org | python3 -

# Windows (PowerShell)
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
```

Po instalacji upewnij się, że Poetry jest dostępne w terminalu:
```bash
poetry --version
```

### 2. Instalacja zależności projektu

Przejdź do głównego katalogu repozytorium (tam gdzie znajduje się `pyproject.toml`) i zainstaluj zależności:

```bash
poetry install
```

Poetry automatycznie utworzy wirtualne środowisko i zainstaluje wszystkie wymagane biblioteki.

### 3. Konfiguracja tokenu API Duffel

Aby korzystać z systemu, potrzebujesz tokenu API Duffel:

1. **Załóż konto na Duffel:** https://duffel.com/ (darmowe konto developerskie)
2. **Pobierz token testowy** z panelu developera (zaczyna się od `duffel_test_...`)
3. **Utwórz plik `.env`** w katalogu `Duffel_api/`:

```bash
# Duffel_api/.env
DUFFEL_API_TOKEN=duffel_test_TWOJ_TOKEN_TUTAJ
```

**WAŻNE:** Plik `.env` zawiera wrażliwe dane i **NIE powinien być commitowany** do repozytorium. Jest już dodany do `.gitignore`.

---

## Uruchamianie

### Skaner (Backend) - Pobieranie danych

Skaner iteruje przez wszystkie pary miast z pliku `europe_cities.json`, pobiera dane o lotach z API Duffel i zapisuje je do bazy SQLite.

```bash
poetry run python Duffel_api/run_scanner.py
```

**Cechy skanera:**
- **Wznawialność:** Można przerwać skrypt (`Ctrl+C`) i wznowić później - już zeskanowane trasy nie będą ponownie pobierane
- **Logowanie:** Wszystkie operacje są zapisywane do pliku `scanner.log`
- **Rate Limiting:** Automatyczny backoff przy limitach API (błędy 429)
- **Filtrowanie:** Zapisywane są tylko loty bezpośrednie (non-stop)

**Przykładowy output:**
```
2025-12-21 10:15:32 - __main__ - INFO - Flight Scanner starting
2025-12-21 10:15:32 - __main__ - INFO - Loaded 4 cities from configuration
2025-12-21 10:15:32 - __main__ - INFO - Route pairs to scan: 12
2025-12-21 10:15:32 - __main__ - INFO - Scan range: 2026-07-13 - 2026-07-19 (7 days)
2025-12-21 10:15:33 - __main__ - INFO - [1/12] Scanning week for: WAW -> LON
2025-12-21 10:15:35 - __main__ - INFO -    2026-07-13: +8 offers saved
...
```

### Dashboard (Frontend) - Wizualizacja

Dashboard to interaktywna aplikacja webowa do analizy zebranych danych.

```bash
poetry run streamlit run Duffel_api/dashboard.py
```

Aplikacja uruchomi się automatycznie w przeglądarce pod adresem: `http://localhost:8501`

**Funkcje dashboardu:**

| Zakładka | Opis |
|----------|------|
| **Price Analysis** | Wykresy średnich cen wg dnia tygodnia, rozkład cen wg linii, kalendarz cenowy |
| **Quality & Comfort** | Macierz wartości (cena vs komfort), ranking ekologicznych połączeń |
| **Route Map** | Interaktywna mapa Europy z siecią połączeń |
| **Detailed Data** | Tabela ze wszystkimi ofertami z możliwością sortowania i filtrowania |

**Filtry w sidebarze:**
- Lotnisko wylotu
- Lotniska docelowe (multiselect)
- Zakres dat
- Linie lotnicze
- Maksymalna cena
- Wymagane WiFi / bagaż rejestrowany

---

## Schemat Bazy Danych (SQLite)

Baza `flights.db` składa się z trzech powiązanych tabel, które dokładnie odwzorowują strukturę danych z API Duffel.

### 1. `flights_static` (Katalog Produktów)

Przechowuje unikalne, niezmienne cechy połączenia lotniczego. Rekord jest unikalny dla kombinacji: `Przewoźnik + NrLotu + Skąd + Dokąd`. Służy do deduplikacji danych o jakości i logistyce.

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `route_id` | TEXT (PK) | Unikalny klucz trasy (np. `LO285-WAW-LHR`) |
| `carrier_code` | TEXT | Kod IATA linii lotniczej (np. `LO`) |
| `carrier_name` | TEXT | Pełna nazwa przewoźnika (np. `LOT Polish Airlines`) |
| `flight_number` | TEXT | Numer lotu (np. `0285`) |
| `origin_iata` / `dest_iata` | TEXT | Kody IATA lotnisk (np. `WAW`, `LHR`) |
| `dest_city_code` | TEXT | Kod IATA miasta docelowego (np. `LON`) |
| `duration_iso` | TEXT | Czas trwania lotu w formacie ISO 8601 (np. `PT2H35M`) |
| `origin_lat` / `lon` | REAL | Współrzędne GPS lotniska wylotu (do map) |
| `dest_lat` / `lon` | REAL | Współrzędne GPS lotniska przylotu (do map) |
| `aircraft_model` | TEXT | Model samolotu (np. `Boeing 737 MAX 8`) |
| `has_wifi` | BOOL | Czy na pokładzie jest dostępne Wi-Fi? |
| `has_power` | BOOL | Czy dostępne jest zasilanie (gniazdka/USB)? |
| `seat_pitch` | TEXT | Odległość między fotelami (np. `29 inches`) |
| `legroom` | TEXT | Opisowa ilość miejsca na nogi (np. `standard`, `extra`) |
| `co2_kg` | REAL | Szacowana emisja CO2 dla tego lotu (w kg) |
| `logo_url` | TEXT | URL do logotypu linii lotniczej |
| `is_non_stop` | BOOL | `TRUE` jeśli lot jest bez międzylądowań technicznych |
| `operating_days` | TEXT | Dni tygodnia, w które lot operuje (np. `"1,3,5"`) |

### 2. `flight_quotes` (Historia Cen)

Przechowuje dynamiczne "migawki" cenowe (oferty) dla danego połączenia w konkretnym dniu.

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `id` | INT (PK) | Autoinkrementacja |
| `flight_static_id` | TEXT (FK) | Relacja do tabeli `flights_static` |
| `price_amount` | REAL | Całkowita cena biletu |
| `currency` | TEXT | Waluta oferty (np. `EUR`, `PLN`) |
| `fare_brand` | TEXT | Nazwa taryfy (np. `Economy Light`, `Flex`) |
| `baggage_checked` | INT | Liczba sztuk bagażu rejestrowanego w cenie |
| `baggage_carryon` | INT | Liczba sztuk bagażu podręcznego w cenie |
| `departure_date` | TEXT | Data i godzina wylotu (ISO 8601) |
| `scanned_at` | TEXT | Data i godzina pobrania danych (Timestamp skanowania) |

### 3. `route_scan_status` (Logika Sterowania)

Tabela techniczna zapobiegająca ponownemu skanowaniu tych samych par miast po restarcie skryptu.

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `origin` / `destination` | TEXT (PK) | Para miast (np. `WAW` -> `MAD`) |
| `status` | TEXT | `COMPLETED` tylko po w pełni udanym skanie całego tygodnia |
| `last_scanned_at` | DATETIME | Data ostatniej próby skanowania |
| `total_flights_found` | INT | Liczba unikalnych lotów znaleziona dla tej pary |

---

## Kluczowe Mechanizmy

### Algorytm "Weekly Scanner"

W pliku `run_scanner.py` pętla skanowania sprawdza 7 dni po kolei dla każdej pary miast.

* **Logika:** Jeśli API zwróci błąd dla Środy, flaga `all_days_successful` zmienia się na `False`.
* **Konsekwencja:** Para miast **nie** dostaje statusu `COMPLETED` w bazie. Przy następnym uruchomieniu skrypt sprawdzi tę parę od nowa (od Poniedziałku). Gwarantuje to spójność danych.

### Obsługa Rate Limit (429)

W pliku `run_scanner.py` zaimplementowano **Exponential Backoff**.

* Jeśli Duffel zwróci błąd 429 (Too Many Requests):
1. Skrypt czeka 5 sekund.
2. Jeśli błąd się powtarza -> 10 sekund -> 15 sekund.
3. Po 3 błędach pod rząd przerywa skanowanie danej pary, aby nie dostać bana.

### Deduplikacja i Scalanie (Database)

W pliku `core/database.py` metoda `save_route` używa logiki **Check & Merge**:

* Zamiast nadpisywać rekord statyczny, pobiera istniejące `operating_days` (np. `"1"` - Poniedziałek).
* Dodaje nowy dzień (np. `"3"` - Środa).
* Zapisuje posortowany wynik (`"1,3"`) z powrotem do bazy.

---

## Rozwiązywanie Problemów

### Token API nie działa

```
ValueError: API token not found. Please check your .env file...
```

**Rozwiązanie:**
1. Upewnij się, że plik `.env` znajduje się w katalogu `Duffel_api/`
2. Sprawdź format: `DUFFEL_API_TOKEN=duffel_test_...` (bez cudzysłowów)
3. Zrestartuj skrypt po zmianie `.env`

### Dashboard pokazuje "Database is empty"

```
Database is empty. Run 'run_scanner.py' first!
```

**Rozwiązanie:**
Najpierw uruchom skaner, aby zebrać dane:
```bash
poetry run python Duffel_api/run_scanner.py
```

### Błędy Rate Limit (429) - skaner zatrzymuje się

**Rozwiązanie:**
- To normalne zachowanie przy dużej liczbie zapytań
- Skaner automatycznie czeka i wznawia pracę
- Można bezpiecznie przerwać (`Ctrl+C`) i wznowić później

### Brak modułu / ImportError

```
ModuleNotFoundError: No module named 'streamlit'
```

**Rozwiązanie:**
```bash
poetry install
```

---

## Dodawanie Nowych Miast

Aby rozszerzyć skanowanie o nowe miasta europejskie:

1. Otwórz plik `data/europe_cities.json`
2. Dodaj kody IATA miast (nie lotnisk!) do listy:

```json
[
  "WAW", "LON", "PAR", "ROM",
  "BCN", "BER", "AMS", "VIE"
]
```

**Uwaga:** Używaj kodów **miast** (np. `LON` dla Londynu), nie lotnisk (np. `LHR`). API Duffel automatycznie zwróci loty do wszystkich lotnisk w danym mieście.

Lista popularnych kodów IATA miast europejskich:
- `LON` - Londyn (LHR, LGW, STN, LTN, LCY)
- `PAR` - Paryż (CDG, ORY)
- `BER` - Berlin (BER)
- `ROM` - Rzym (FCO, CIA)
- `BCN` - Barcelona (BCN)
- `AMS` - Amsterdam (AMS)
- `VIE` - Wiedeń (VIE)
- `PRG` - Praga (PRG)
- `BUD` - Budapeszt (BUD)

---

## Roadmap / To-Do

Rzeczy do zrobienia w kolejnych etapach rozwoju:

* [ ] **Rozszerzenie listy miast:** Obecnie `europe_cities.json` to MVP. Trzeba dodać ~100 największych miast Europy.
* [ ] **Reverse Route Inference:** Jeśli znamy lot `WAW -> LHR`, możemy z 99% pewnością założyć istnienie `LHR -> WAW` (chociaż godziny będą inne). Można to wykorzystać do optymalizacji zapytań.
* [ ] **Deploy:** Umieszczenie bazy i dashboardu w chmurze (np. Streamlit Cloud + plik DB na S3 lub Azure SQL).

---

*Dokumentacja zaktualizowana: Grudzień 2025*