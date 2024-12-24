# **ETL proces datasetu NorthWind**
Toto úložisko obsahuje implementáciu procesu ETL softvéru Snowflake na analýzu údajov zo súboru údajov **NorthWind**. Proces zahŕňa kroky na extrakciu, transformáciu a načítanie údajov o objednávkach, zákazníkoch, kategóriách, produktoch, zamestnancoch a ďalších relevantných entitách do dimenzionálneho modelu. Výsledný dátový model umožňuje viacrozmernú analýzu a vizualizáciu kľúčových ukazovateľov.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa predaja, zákazníkov a ich objednávok. Táto analýza umožňuje identifikovať trendy v predajných preferenciách, najpredávanejšie produkty a správanie zákazníkov.

Zdrojové údaje [tu] (https://github.com/microsoft/sql-server-samples/tree/master/samples/databases). 
Súbor údajov obsahuje päť hlavných tabuliek:
- `categories`
- `products`
- `suppliers`
- `shippers`
- `orderdetails`
- `orders`
- `customers`
- `employees`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.  

---
### **1.1 Dátová architektúra**

### **ERD diagram**
ERD diagram **NothWind**:

<p align="center">
  <img src="https://github.com/Za-Ant/northwind_ostrich/blob/master/Northwind_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma NothWind</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, ktorý umožňuje efektívnu analýzu údajov z Northwind databázy. Centrálnu časť predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_products`**: Obsahuje podrobné informácie o produktoch (názov, jednotka, cena, názov kategórie, popis kategórie).
- **`dim_customers`**: Obsahuje demografické údaje o zákazníkoch (meno, adresa, mesto, PSČ, krajina).
- **`dim_suppliers`**: Obsahuje údaje o dodávateľoch (meno, adresa).
- **`dim_employees`**: Obsahuje informácie o zamestnancoch, ktorí spracovali objednávky (meno).
- **`dim_dates`**: Obsahuje podrobné údaje o dátumoch objednávok (deň, mesiac, rok, deň v týždni).
- **`dim_addresses`**: Obsahuje údaje o adresách (adresa, mesto, PSČ, krajina).

Pre reláciu medzi produktmi a objednávkami bola použitá prepojovacia tabuľka bridge_orders_products, ktorá umožňuje správne mapovanie relácie N:M medzi objednávkami a produktmi.

Struktúra hviezdicového modelu je znázornená na diagrame. Tento model je optimalizovaný pre analýzu predajných trendov, správania zákazníkov a výkonnosti produktov.

<p align="center">
  <img src="https://github.com/Za-Ant/northwind_ostrich/blob/master/starscheme.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre NothWind</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
