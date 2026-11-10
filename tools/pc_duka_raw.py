"""Surowe dzienne świece M1 BID/ASK z Dukascopy do katalogu (PC właściciela ALBO VPS). TYLKO stdlib (venv PC bez pandas).
Uruchom: python pc_duka_raw.py <katalog_roboczy> <SYMBOL> [--watki=N] [--od=RRRR-MM] [--do=RRRR-MM] [--odwrotnie] [--tempo=R]
Zapis: <katalog>/chunks/RRRR/MM/DD/{BID,ASK}_candles_min_1.bi5 (MM 0-indeksowane jak URL; 404 → pusty plik .404);
chunks/ bo `pc_job.sh fetch --all` kopiuje tylko ten podkatalog. Idempotentne: istniejący plik = checkpoint (dzień, nie miesiąc).
Dalej: tools/duka_ticks.py --z-katalogu=<dir>/chunks --sym=<SYMBOL> (dekoduje na VPS, pomija gotowe miesiące).

Pomiar 10.11 (VPS i PC, docs/NOTATNIK_CENTRALI.md §8): Dukascopy ogranicza TEMPO żądań na adres IP — przy odstępie
< ~1 s odpowiada natychmiastowym 503 (bez Retry-After), przy ≥ 1 s żądanie/s daje 200 (0,2–3 s; pierwsze z origin 15 s).
Stary pobierak (sleep 0,15 s, po 503 odczekanie 30…180 s × 8 prób) marnował > 90 % czasu w odczekaniach i tracił cały
miesiąc przy jednym nieudanym dniu. Tu: wspólny regulator tempa (AIMD): 503 → tempo ×0,5 i krótka pauza, pasmo 200 → +10 %;
N wątków dzieli jedno pasmo, więc liczba wątków nie zwiększa agresji wobec serwera (limit prób = 40, bez porzucania dnia).
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

END = date(2025, 10, 19)
START = date(2015, 1, 1)
UA = {"User-Agent": "Mozilla/5.0"}


class Regulator:
    """Jedno pasmo żądań/s dla wszystkich wątków. 503/429 → tempo ×0,5 (min 0,2/s) + pauza 2 s; 20 kolejnych 200 → ×1,1 (max tempo_max)."""

    def __init__(self, tempo, tempo_max):
        self.tempo, self.max = tempo, tempo_max
        self.lock, self.nast, self.ok, self.pauza_do = threading.Lock(), time.monotonic(), 0, 0.0
        self.n503 = 0

    def czekaj(self):
        with self.lock:
            t = max(time.monotonic(), self.nast, self.pauza_do)
            self.nast = t + 1.0 / self.tempo
        time.sleep(max(0.0, t - time.monotonic()))

    def zle(self):
        with self.lock:
            self.n503 += 1
            self.ok = 0
            self.tempo = max(0.2, self.tempo * 0.5)
            self.pauza_do = time.monotonic() + 2.0

    def dobrze(self):
        with self.lock:
            self.ok += 1
            if self.ok >= 20:
                self.ok, self.tempo = 0, min(self.max, self.tempo * 1.1)


def get(url, reg, tries=40):
    """bajty, b'' dla 404, None po wyczerpaniu prób (503/429 = za szybko → regulator zwalnia, ponawiamy ten sam URL)."""
    for k in range(tries):
        reg.czekaj()
        try:
            b = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60).read()
            reg.dobrze()
            return b
        except urllib.error.HTTPError as e:
            if e.code == 404:
                reg.dobrze()
                return b""
            if e.code in (503, 429):
                reg.zle()
            else:
                time.sleep(3)
        except Exception:
            time.sleep(min(3 * (k + 1), 30))
    return None


def main(root, sym, watki, od, do, odwrotnie, tempo, tempo_max):
    base = f"https://datafeed.dukascopy.com/datafeed/{sym}/"
    ch = os.path.join(root, "chunks")
    a = max(START, od) if od else START
    b = min(END, do) if do else END
    dni = [a + timedelta(days=i) for i in range((b - a).days + 1)]
    dni = [d for d in dni if d.weekday() != 5]
    if odwrotnie:
        dni.reverse()
    reg = Regulator(tempo, tempo_max)
    t0 = time.time()
    stan = {"zrobione": 0, "bledy": 0, "pominiete": 0, "i": 0}
    lock = threading.Lock()

    def dzien(d):
        kat = os.path.join(ch, f"{d.year}", f"{d.month - 1:02d}", f"{d.day:02d}")
        os.makedirs(kat, exist_ok=True)
        z, bl, pom = 0, 0, 0
        for strona in ("BID", "ASK"):
            p = os.path.join(kat, f"{strona}_candles_min_1.bi5")
            if os.path.exists(p) or os.path.exists(p + ".404"):
                pom += 1
                continue
            raw = get(f"{base}{d.year}/{d.month - 1:02d}/{d.day:02d}/{strona}_candles_min_1.bi5", reg)
            if raw is None:
                bl += 1
                continue
            if not raw:
                open(p + ".404", "wb").close()
            else:
                tmp = p + ".part"
                with open(tmp, "wb") as f:
                    f.write(raw)
                os.replace(tmp, p)
            z += 1
        with lock:
            stan["zrobione"] += z
            stan["bledy"] += bl
            stan["pominiete"] += pom
            stan["i"] += 1
            if stan["i"] % 20 == 0 or stan["i"] == len(dni):
                h = max((time.time() - t0) / 3600, 1e-9)
                with open(os.path.join(root, "progress.json.part"), "w") as f:
                    json.dump({"dzien": d.isoformat(), "i": stan["i"], "z": len(dni), "pobrane_pliki": stan["zrobione"],
                               "bledy": stan["bledy"], "juz_byly": stan["pominiete"], "pliki_na_h": round(stan["zrobione"] / h, 1),
                               "tempo_na_s": round(reg.tempo, 2), "n503": reg.n503, "watki": watki}, f)
                os.replace(os.path.join(root, "progress.json.part"), os.path.join(root, "progress.json"))
                print(f"{d} {stan['i']}/{len(dni)} pliki {stan['zrobione']} ({stan['zrobione'] / h:.0f}/h) 503: {reg.n503} tempo {reg.tempo:.2f}/s", flush=True)

    with ThreadPoolExecutor(watki) as ex:
        list(ex.map(dzien, dni))
    if stan["bledy"] == 0:
        with open(os.path.join(root, "done.json"), "w") as f:
            json.dump({"sym": sym, "dni": len(dni), "od": a.isoformat(), "do": b.isoformat()}, f)
    print("koniec", sym, "błędy", stan["bledy"], "pliki", stan["zrobione"], flush=True)
    return stan["bledy"]


def _mies(s):
    r, m = s.split("-")
    return date(int(r), int(m), 1)


if __name__ == "__main__":
    poz = [x for x in sys.argv[1:] if not x.startswith("--")]
    opc = dict(x[2:].split("=", 1) if "=" in x else (x[2:], "1") for x in sys.argv[1:] if x.startswith("--"))
    os.makedirs(poz[0], exist_ok=True)
    do = _mies(opc["do"]) if "do" in opc else None
    if do:                                   # --do=RRRR-MM włącznie: ostatni dzień miesiąca
        do = (do.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    kod = main(poz[0], poz[1], int(opc.get("watki", 4)), _mies(opc["od"]) if "od" in opc else None, do,
               "odwrotnie" in opc, float(opc.get("tempo", 1.0)), float(opc.get("tempo_max", 4.0)))
    sys.exit(1 if kod else 0)
