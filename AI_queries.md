# Historia zapytań do AI

## Milestone 1

### Krystian Głuszko

- Czy zamiast PM2.5 lepiej przewidywać arsen i rtęć (albo inny bardzo szkodliwy i powszechny wskaźnik), ile danych mają te wskaźniki i czy model może przewidywać dwie zmienne naraz? AI porównało wskaźniki w bronze (arsen skupiony przy hutach, rtęć na 5 stacjach, benzo(a)piren na 174 stacjach).
- Co to jest próg wykrywalności i czy wartości 0.5 (As) i 0.05 (BaP) to na pewno wartości poniżej progu? AI sprawdziło wytyczne EEA i flagi walidacji w danych EEA dla polskich stacji (flaga 3: połowa progu wykrywalności) oraz wykryło próbki tygodniowe BaP.
- Czy przewidywanie BaP jest zgodne z kryteriami własnego zbioru, skoro to średnia tygodniowa? Wynik: za mało wierszy pod model (około 9 tys. pomiarów), zostajemy przy PM2.5; przy okazji dopisana anomalia 8.17 (kod stacji ze spacją).


### Wiktor Kozikowski

- Czy dane GIOŚ wystarczą do modelu prognozującego PM2.5 na podstawie pogody? Czego brakuje i skąd to pobrać?
- Czy dane Open-Meteo były przeliczane na godziny lub inną strefę czasową?
- Sprawdź w dokumentacji Open-Meteo, jak wybierana jest komórka siatki i skąd stały kierunek wiatru przy ciszy.
- pytam AI o informacje na temat wykrytych anomali i wyszukajnie przydanych informacji które mogły by wytłumaczyć ldaczego występują 

### Jakub Glugla

- Poproszono AI o przygotowanie komendy do CMD służącej do utworzenia struktury projektu.
- Wykorzystano AI do dokładniejszego przeanalizowania podstawowego przepływu danych w projekcie.
- Poproszono AI o utworzenie podstawy pliku README, która została następnie sprawdzona i delikatnie poprawiona.

## Milestone 2

### Krystian Głuszko

- W którym momencie kończą się pomiary godzinowe w przypadku GIOS a w którym OPEN METEO. Czy Występują między nimi różnicę? Żeby nie przyrównywać przyszłej pogody z pomiarem z GIOS sprzed godziny
- Czy usunięcie wartości NULL może przynieść realną korzyść dla przyszłego modelu predykcyjnego przewidującego PM 2.5?

### Wiktor Kozikowski

-

### Jakub Glugla

-

## Milestone 3

### Krystian Głuszko

-

### Wiktor Kozikowski

-

### Jakub Glugla

-
