Sei incaricato di configurare un ambiente Apache Iceberg utilizzando PyIceberg per gestire dati in un warehouse locale. L'obiettivo è simulare uno scenario pratico in cui inizializzi e configuri un catalogo PyIceberg e crei uno namespace per organizzare le tabelle.

## Parte 1

Aprire lo script Python chiamato `soluzione.py` e scrivere il codice nella parte 1 che esegua i seguenti compiti:

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
python soluzione.py

# validare che l'output sia corretto lanciando
python validazione_1.py
```

Se alla fine vedrai stampato

> Validation successful.

la soluzione è corretta.

## Parte 2

Scrivere nella sezione parte 2 dello script `soluzione.py` il codice che esegue i seguenti compiti:

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
python soluzione.py

# validare che l'output sia corretto lanciando
python validazione_2.py
```

Se alla fine vedrai stampato

> Validation successful.

la soluzione è corretta.

PS se riscontri dei warning, puoi ignorarli.

## Parte 3

Scrivi la parte 3 dello script `soluzione.py` che esegue i seguenti compiti:

1. Aggiungi nuovi dati:
  - Inserisci i seguenti dati nella tabella esistente:

| id  | name  | hire_date  |
| --- | ----- | ---------- |
| 4   | David | 2020-01-04 |
| 5   | Eve   | 2020-01-05 |

2. Conta gli Snapshot:
  - Implementa una funzione Python chiamata `count_table_snapshots` che accetta come input una tabella Iceberg e restituisce il numero di snapshot attualmente registrati per quella tabella.

### Verificare che funzioni

Per verificare che la soluzione sia corretta

```bash
# posizionarsi nella cartella dello script Python e lanciare lo script
# lo script deve girare senza errori
python soluzione.py

# validare che l'output sia corretto lanciando
python validazione_3.py
```

Se alla fine vedrai stampato

> Validation successful.

la soluzione è corretta.

PS se riscontri dei warning, puoi ignorarli.

## Parte 4

Scrivi la parte 4 dello script `soluzione.py` che esegue i seguenti compiti:

1. scrivere corpo della funzione `rows_at_second_last_snapshot`:
  - Implementa una funzione chiamata `rows_at_second_last_snapshot` che soddisfi i seguenti requisiti:
    - Prende come input un oggetto `Table` di PyIceberg.
    - Restituisce il numero di righe contenute nel **penultimo** snapshot della tabella.
2. stampare il risultato della chiamata alla funzione per verificare che funzioni con la `table` dei punti precedenti.


### Verificare che funzioni

Per verificare che la soluzione sia corretta

```bash
# posizionarsi nella cartella dello script Python e lanciare lo script
# lo script deve girare senza errori
python soluzione.py

# validare che l'output sia corretto lanciando
python validazione_4.py
```

Se alla fine vedrai stampato

> Validation successful.

la soluzione è corretta.

PS se riscontri dei warning, puoi ignorarli.