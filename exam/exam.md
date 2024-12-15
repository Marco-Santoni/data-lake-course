Sei incaricato di configurare un ambiente Apache Iceberg utilizzando PyIceberg per gestire dati in un warehouse locale. L'obiettivo è simulare uno scenario pratico in cui inizializzi e configuri un catalogo PyIceberg e crei uno namespace per organizzare le tabelle.

## Parte 1

Scrivi uno script Python chiamato `soluzione_1.py` che esegua i seguenti compiti:

1. Pulizia e Preparazione dell'Ambiente:
  - Definisci una directory locale chiamata `rizzoli_warehouse` per fungere da warehouse dei dati. Se la directory esiste già, eliminala per iniziare con un ambiente pulito. Crea la directory se non esiste.
2. Inizializza un Catalogo SQL di PyIceberg:
  - Configura un catalogo Iceberg SQL chiamato `acme_corp` utilizzando SQLite come database di appoggio. Assicurati che il file del database SQLite si trovi nella directory `rizzoli_warehouse` e che il catalogo sia configurato per utilizzare la stessa directory come warehouse dei dati.
3. Crea uno Namespace:
- All'interno del catalogo, crea uno namespace chiamato `registry`, che potrà essere utilizzato per organizzare le tabelle.

### Verificare che funzioni

Per verificare che la soluzione sia corretta

```bash
# posizionarsi nella cartella dello script Python e lanciare lo script
# lo script deve girare senza errori
python soluzione_1.py

# validare che l'output sia corretto lanciando
python validazione_1.py
```

Se alla fine vedrai stampato

> Validation successful.

la soluzione è corretta.

## Parte 2

Scrivi uno script Python chiamato `soluzione_1.py` che esegua i seguenti compiti:

1. Crea una tabella Iceberg chiamata  `employees` all'interno del namespace `registry` con i seguenti campi:
  - `id`: Un numero intero `long`, non obbligatorio.
  - `name`: Una stringa, non obbligatoria.
  - `hire_date`: Una data, non obbligatoria.
2. Popola la tabella inserendo i seguenti dati nella tabella:

| id  | name    | hire_date  |
| --- | ------- | ---------- |
| 1   | Alice   | 2020-01-01 |
| 2   | Bob     | 2020-01-02 |
| 3   | Charlie | 2020-01-03 |

Inserire i dati in un'unica istruzione in modo che sia creato un unico nuovo snapshoot della tabella.

### Verificare che funzioni

Per verificare che la soluzione sia corretta

```bash
# posizionarsi nella cartella dello script Python e lanciare lo script
# lo script deve girare senza errori
python soluzione_2.py

# validare che l'output sia corretto lanciando
python validazione_2.py
```

Se alla fine vedrai stampato

> Validation successful.

la soluzione è corretta.
