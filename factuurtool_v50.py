
import streamlit as st
import pandas as pd
import pdfplumber
import re
from pdf2image import convert_from_path
import pytesseract
from io import BytesIO
import tempfile
import os
import sqlite3
from datetime import datetime
from pathlib import Path
import platform
import shutil

def clean_ocr_noise(s: str) -> str:
    if not s:
        return s
    s = s.replace("m?", "m2").replace("M?", "m2").replace("m^2", "m2").replace("m°", "m2")
    s = s.replace("O,", "0,").replace("O.", "0.")
    return s



_RAPIDFUZZ_OK = False  # Fuzzy matching uitgeschakeld in deze versie

# ==== Optionele SharePoint client ====
# Wordt alleen gebruikt als je "Bron = SharePoint" kiest.
_SP_OK = False
try:
    from office365.sharepoint.client_context import ClientContext
    from office365.runtime.auth.user_credential import UserCredential
    _SP_OK = True
except Exception:
    _SP_OK = False

# === INSTELLINGEN ===
# (Aangepast voor Sem) – maak paden OS-agnostisch en veilig
TESSERACT_PATH = r"C:\Users\Sem Kosse\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"
POPLER_PATH = r"C:\poppler\poppler-24.08.0\Library\bin"  # Windows-poppler pad, val terug naar None als niet aanwezig

# Kies tesseract bin: op Windows het vaste pad als het bestaat; anders via PATH als beschikbaar
if os.name == "nt" and os.path.exists(TESSERACT_PATH):
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH
else:
    auto_tesseract = shutil.which("tesseract")
    if auto_tesseract:
        pytesseract.pytesseract.tesseract_cmd = auto_tesseract

# Alleen een poppler_path meegeven als het pad bestaat (voorkomt fouten op Linux/macOS)
if not (os.name == "nt" and os.path.isdir(POPLER_PATH)):
    POPLER_PATH = None

# Pas de paginatitel aan naar huidige versie
# Update de paginatitel voor versie v49
st.set_page_config(page_title="Factuurcontrole Tool (Trevian) v49 – auto-import (no-fuzzy, lichte historie)", layout="wide")

# === STYLES ===
st.markdown(
    """
    <style>
      .trevian-blue { color: #1C4C96; font-weight: bold; }
      .header-box { background-color: #1C4C96; padding: 1rem; border-radius: 0.5rem; color: white; }
      .main { background-color: #f9f9f9; }
      .block-container { padding-top: 2rem; }
      h1, h2, h3 { color: #1C4C96; }
      .stButton>button, .stDownloadButton>button { background-color: #1C4C96; color: white; }
      .stProgress > div > div { background-color: #1C4C96; }
    </style>
    """,
    unsafe_allow_html=True,
)

# === HEADER ===
col1, col2 = st.columns([1, 6])
with col1:
    st.image("trevian finance logo.jpg", width=140)
with col2:
    st.markdown('<h1 style="color:#1C4C96; margin-bottom:0;">Factuurcontrole Tool</h1>', unsafe_allow_html=True)
    # Werk de versieaanduiding bij naar v49
    st.markdown('<p style="color:#888;">Onderdeel van Trevian Finance & Control — v49 (auto-import)</p>', unsafe_allow_html=True)

st.markdown("---")

# === SCAN INSTELLINGEN (links boven) ===
# Deze sectie plaatst de scan-knop en automatische scan-opties buiten de sidebar, links boven op de pagina.
scan_col, _ = st.columns([2, 4])
with scan_col:
    st.markdown("### 🔍 Scannen")
    # Direct scannen van geselecteerde bron
    scan_now_top = st.button("🔎 Nu scannen")
    # Automatische scan inschakelen
    enable_autorun_top = st.checkbox("Automatisch elke X minuten scannen", value=False)
    interval_min_top = st.number_input("Interval (minuten)", min_value=5, max_value=180, value=30, step=5)

# Kopieer de waarden naar variabelen die elders in de code gebruikt worden
scan_now = scan_now_top
enable_autorun = enable_autorun_top
interval_min = interval_min_top

# ========== HULP: DB (ook voor double-processing voorkomen) ==========

def init_db(db_path: str):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            label TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            bestandsnaam TEXT,
            taakcode_gevonden TEXT,
            taakcode_gematcht TEXT,
            fuzzy_score REAL,
            aantal_geschat REAL,
            omschrijving TEXT,
            totaalprijs_boek REAL,
            verwacht_bedrag REAL,
            prijs_op_factuur REAL,
            afwijking REAL,
            status TEXT,
            regels TEXT,
            verwerkingsmethode TEXT,
            FOREIGN KEY(run_id) REFERENCES runs(id)
        )
        """
    )
    # onthoud reeds verwerkte bestanden (hash of bestandsnaam + modified time)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ingested_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE,
            mtime REAL
        )
        """
    )
    con.commit()
    return con

def is_already_ingested(db_path: str, path: str, mtime: float) -> bool:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("SELECT mtime FROM ingested_files WHERE path = ?", (path,))
    row = cur.fetchone()
    con.close()
    return bool(row and abs(row[0] - mtime) < 1e-6)

def mark_ingested(db_path: str, path: str, mtime: float):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO ingested_files(path, mtime) VALUES(?, ?)", (path, mtime))
    con.commit()
    con.close()

def save_run_and_results(db_path: str, run_label: str, df: pd.DataFrame):
    con = init_db(db_path)
    cur = con.cursor()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("INSERT INTO runs (ts, label) VALUES (?, ?)", (ts, run_label or None))
    run_id = cur.lastrowid

    insert_cols = [
        "Bestandsnaam", "Taakcode_gevonden", "Taakcode", "Fuzzy_score", "Aantal (geschat)", "Omschrijving",
        "Totaalprijs boek", "Verwacht bedrag", "Prijs op factuur (som)", "Afwijking", "Status", "Regels", "Verwerkingsmethode"
    ]

    for _, row in df[insert_cols].iterrows():
        cur.execute(
            """
            INSERT INTO results (
                run_id, bestandsnaam, taakcode_gevonden, taakcode_gematcht, fuzzy_score,
                aantal_geschat, omschrijving, totaalprijs_boek, verwacht_bedrag,
                prijs_op_factuur, afwijking, status, regels, verwerkingsmethode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                row["Bestandsnaam"],
                row["Taakcode_gevonden"],
                row["Taakcode"],
                None if pd.isna(row.get("Fuzzy_score", None)) else float(row.get("Fuzzy_score")),
                float(row["Aantal (geschat)"]) if pd.notna(row["Aantal (geschat)"]) else None,
                row["Omschrijving"],
                float(row["Totaalprijs boek"]) if pd.notna(row["Totaalprijs boek"]) else None,
                float(row["Verwacht bedrag"]) if pd.notna(row["Verwacht bedrag"]) else None,
                float(row["Prijs op factuur (som)"]) if pd.notna(row["Prijs op factuur (som)"]) else None,
                float(row["Afwijking"]) if pd.notna(row["Afwijking"]) else None,
                row["Status"],
                row["Regels"],
                row["Verwerkingsmethode"],
            ),
        )
    con.commit()
    con.close()
    return run_id



def extract_factuurnummer(tekst: str, filename: str = "") -> str:
    if not tekst:
        tekst = ""
    patterns = [
        r"factuurnummer\s*[:#]?\s*([A-Z0-9\-/]{5,})",
        r"factuur\s*nr\.?\s*[:#]?\s*([A-Z0-9\-/]{5,})",
        r"factuurnr\.?\s*[:#]?\s*([A-Z0-9\-/]{5,})",
        r"invoice\s*(?:no|nr|number)\s*[:#]?\s*([A-Z0-9\-/]{5,})",
        r"kenmerk\s*[:#]?\s*([A-Z0-9\-/]{5,})",
    ]
    low = (tekst or "").lower()
    for pat in patterns:
        m = re.search(pat, low, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip().rstrip('.')
    base = (filename or "").split('/')[-1]
    m = re.search(r"(\d{6,})", base)
    return m.group(1) if m else base
# ========== OCR & PARSING HELPERS ==========

def ocr_extract_regels_en_codes(pdf_path, poppler_path):
    tekst = ""
    if poppler_path:
        images = convert_from_path(pdf_path, dpi=200, fmt="png", poppler_path=poppler_path)
    else:
        images = convert_from_path(pdf_path, dpi=200, fmt="png")
    for image in images:
        tekst += pytesseract.image_to_string(image, config="--psm 6")
    regels = tekst.splitlines()
    raw_codes = re.findall(r"[0-9][0-9\s\-\.]{4,}[0-9]", tekst)
    norm_codes = [re.sub(r"\D", "", c).lstrip("0") for c in raw_codes]
    unieke_codes = sorted({c for c in norm_codes if 5 <= len(c) <= 12})
    return regels, unieke_codes



def extract_bedragen_with_flags(tekstregel):
    """Like extract_bedragen maar geeft (waarde, has_euro, has_unit) per match terug.
    Wordt gebruikt om kleine waarden zonder € weg te filteren (zoals '1,00 stu').
    """
    out = []
    if not tekstregel:
        return out
    s = clean_ocr_noise(tekstregel)
    unit_pat = r"(?:m2|m³|m3|\bm\b|meter|stu?k?s?|st\b|wk\b|uur\b|hrs?\b|kg\b|l\b|liter\b)"
    rx = re.compile(rf"(?P<euro>(?:€|eur)\s*)?(?P<num>-?\d{{1,3}}(?:[.\s]\d{{3}})*(?:[.,]\d{{2}})|-?\d+(?:[.,]\d{{2}}))(?!\d)(?P<unit>\s*(?:{unit_pat}))?", re.IGNORECASE)
    for m in rx.finditer(s):
        euro = bool(m.group('euro'))
        unit = bool((m.group('unit') or '').strip())
        raw = m.group('num').replace('\xa0',' ').replace(' ', '')
        if ',' in raw and '.' in raw:
            if raw.rfind(',') > raw.rfind('.'):
                raw = raw.replace('.', '').replace(',', '.')
            else:
                raw = raw.replace(',', '')
        else:
            raw = raw.replace(',', '.')
        try:
            val = float(raw)
        except Exception:
            continue
        if abs(val) <= 250000:
            out.append((round(val,2), euro, unit))
    return out
def extract_bedragen(tekstregel):
    """
    Haal geldbedragen uit een regel. Voorkeur voor waarden met '€' of 'eur'.
    Getallen die op hoeveelheden lijken (unit er direct achter) of heel klein zijn (≤5) zonder €
    worden genegeerd, ook als elders in de regel wel een € staat.
    """
    res = []
    for val, has_euro, has_unit in extract_bedragen_with_flags(tekstregel):
        if has_unit and not has_euro:
            continue
        if (val <= 5.0) and not has_euro:
            continue
        res.append(val)
    return res


def extract_aantal_beter(tekstregel: str, taakcode: str = None) -> float:
    """
    Extraheer een realistische hoeveelheid uit een regel.
    - Alleen bij expliciete cues: 'x', 'aantal/qty', of VEILIGE units (geen 'u'/'m' enkel-letter).
    - Negeert waarden die identiek zijn aan de (genormaliseerde) taakcode.
    """
    s = (tekstregel or "").lower()

    # Let op: GEEN 'u' of 'm' single-letter units i.v.m. woorden als 'factuur' of 'system'!
    # Voeg losse 'm' als unit toe zodat aantallen zoals '12,00 m' gedetecteerd worden
    # Voeg 'stu' (afkorting voor stuks) toe zodat aantallen zoals '2,00 stu' worden herkend
    # Breid de lijst met eenheden uit zodat alle varianten uit de aangeleverde facturen herkend worden.
    # Naast de al bestaande eenheden (m, m2, m3, stuk, st, stu, etc.) zijn nu ook opgenomen:
    #  - m1  : strekkende meter
    #  - pst : per stuk
    #  - post: forfaitaire post
    #  - wk  : week
    #  - ruimte: per ruimte (vertrek)
    # Opmerking: \b achter de unit zorgt dat het einde van het woord bereikt is, waardoor bv. 'st' in 'stof' niet matcht.
    units = r"(?:m1\b|m2|m\^?2|m3|m\^?3|m²|m³|meter\b|m\b|stuk\b|stuks\b|stk\b|st\b|stu\b|pst\b|post\b|pcs\b|pce\b|set\b|uur\b|hrs\b|hr\b|kg\b|l\b|liter\b|wk\b|week\b|ruimte\b)"

    patterns = [
        # 1) '3 x 50,00' -> 3
        r"(\d+(?:[.,]\d{1,2})?)\s*(?:x|×)\s*\d+(?:[.,]\d{1,2})?",
        # 2) 'aantal: 3' / 'qty 2'
        r"(?:aantal|qty|quantiteit)\s*[:=]?\s*(\d+(?:[.,]\d{1,2})?)",
        # 3) '3 st' / '2,5 m2'
        rf"(\d+(?:[.,]\d{{1,2}})?)\s*{units}",
        # 4) 'st 3'
        rf"\b{units}\s*(\d+(?:[.,]\d{{1,2}})?)",
        # 5) 'x 3'
        r"(?:x|×)\s*(\d+(?:[.,]\d{1,2})?)",
    ]

    taak_norm = None
    if taakcode:
        taak_norm = re.sub(r"\D", "", str(taakcode)).lstrip("0") or None

    for pat in patterns:
        m = re.search(pat, s, flags=re.IGNORECASE)
        if not m:
            continue
        # pak eerste numerieke groep
        for g in m.groups():
            if not g:
                continue
            try:
                q = float(g.replace(",", "."))
            except Exception:
                continue
            # filter absurde aantallen
            if q <= 0 or q > 100000:
                continue
            # voorkom dat de taakcode als aantal wordt gezien
            if taak_norm and re.sub(r"\D", "", str(int(q))) == taak_norm:
                continue
            return q

    # Geen duidelijke aanwijzing gevonden -> 1.0 (conservatief)
    return 1.0


def extract_qty_candidates(tekstregel: str):
    """Geef mogelijke aantallen terug o.b.v. expliciete cues (units/x/aantal)."""
    if not tekstregel:
        return []
    s = tekstregel.lower()

    # Veilige units (geen losse 'u' of 'm')
    # Voeg losse 'm' als unit toe zodat aantallen zoals '3,00 m' worden herkend. Let op: geen losse 'u' omdat dat te veel ruis geeft.
    # Voeg 'stu' toe aan de lijst zodat aantallen met 'stu' (stuk) worden opgepikt
    # Gebruik dezelfde uitgebreide lijst met eenheden als in extract_aantal_beter zodat alle varianten worden herkend.
    units = r"(?:m1\b|m2|m\^?2|m3|m\^?3|m²|m³|meter\b|m\b|stuk\b|stuks\b|stk\b|st\b|stu\b|pst\b|post\b|pcs\b|pce\b|set\b|uur\b|hrs\b|hr\b|kg\b|l\b|liter\b|wk\b|week\b|ruimte\b)"
    cands = []

    # 1) '3 x 50,00' -> 3
    for m in re.finditer(r"(\d+(?:[.,]\d{1,2})?)\s*(?:x|×)\s*\d+(?:[.,]\d{1,2})?", s, flags=re.IGNORECASE):
        cands.append(m.group(1))

    # 2) 'aantal: 3' / 'qty 2'
    for m in re.finditer(r"(?:aantal|qty|quantiteit)\s*[:=]?\s*(\d+(?:[.,]\d{1,2})?)", s, flags=re.IGNORECASE):
        cands.append(m.group(1))

    # 3) '3 st' / '2,5 m2'
    for m in re.finditer(rf"(\d+(?:[.,]\d{{1,2}})?)\s*{units}", s, flags=re.IGNORECASE):
        cands.append(m.group(1))

    # 4) 'st 3'
    for m in re.finditer(rf"\b{units}\s*(\d+(?:[.,]\d{{1,2}})?)", s, flags=re.IGNORECASE):
        cands.append(m.group(1))

    # 5) 'x 3'
    for m in re.finditer(r"(?:x|×)\s*(\d+(?:[.,]\d{1,2})?)", s, flags=re.IGNORECASE):
        cands.append(m.group(1))

    # Normaliseer naar floats, filter ruis
    out = []
    for g in cands:
        try:
            q = float(g.replace(",", "."))
            if 0 < q <= 100000:
                out.append(q)
        except Exception:
            pass
    return out

def pick_qty(tekstregel: str, unit_price: float, bedragen_on_line, taakcode: str = None):
    """Kies het meest waarschijnlijke aantal:
    1) Neem een cue-based kandidaat die NIET gelijk is aan de taakcode.
    2) Als meerdere: kies die waarbij q*unit_price het dichtst bij een bedrag op de regel ligt.
    3) Als geen cues: als er een bedrag is en unit_price > 0, gebruik ratio (bedrag/unit_price).
    4) Anders 1.0.
    """
    cands = extract_qty_candidates(tekstregel)
    # Filter taakcode
    taak_norm = None
    if taakcode:
        taak_norm = re.sub(r"\D", "", str(taakcode)).lstrip("0") or None

    def same_as_task(q):
        return taak_norm and re.sub(r"\D", "", str(int(round(q)))) == taak_norm

    cands = [q for q in cands if not same_as_task(q)]

    # 2) Score t.o.v. bedragen
    if cands and bedragen_on_line:
        best = None
        best_err = None
        for q in cands:
            for b in bedragen_on_line:
                # b kan float/Decimal; cast naar float
                try:
                    bf = float(b)
                except Exception:
                    continue
                expected = q * float(unit_price or 0)
                err = abs(bf - expected)
                if (best is None) or (err < best_err):
                    best, best_err = q, err
        if best is not None:
            return best

    # 3) Geen cues → ratio uit bedrag (pak laatste bedrag op de regel)
    if not cands and bedragen_on_line and unit_price and unit_price > 0:
        try:
            bf = float(bedragen_on_line[-1])
            q = bf / float(unit_price)
            if 0 < q <= 100000:
                # Rond op 2 decimalen voor nette weergave
                return round(q, 2)
        except Exception:
            pass

    # 4) fallback
    return 1.0




def select_regel_bedrag(tekstregel: str, bedragen, expected_total=None):
    if not bedragen:
        return None
    if expected_total is not None:
        best = None
        best_err = None
        for b in bedragen:
            try:
                bf = float(b)
            except Exception:
                continue
            err = abs(bf - float(expected_total))
            if (best is None) or (err < best_err):
                best, best_err = b, err
        return best
    try:
        last = bedragen[-1]
        if abs(float(last)) <= 250000:
            return last
    except Exception:
        pass
    vals = []
    for b in bedragen:
        try:
            bf = float(b)
            if abs(bf) <= 250000:
                vals.append(b)
        except Exception:
            continue
    return max(vals) if vals else None

def choose_line_amount(regel: str, unit_price: float, max_rel_err: float = 0.08, max_abs_err: float = 2.0):
    """Kies (qty, bedrag) per regel die consistent zijn: bedrag ≈ qty * unit_price.
    Vermijd dat aantallen (bijv. '1,00 stu') als bedrag worden gezien.
    """
    if not regel or not unit_price:
        return None, None, None
    triples = extract_bedragen_with_flags(regel)
    if not triples:
        return None, None, None
    qtys = extract_qty_candidates(regel)
    # Filter: houd bedragen met € altijd; zonder € alleen als >= min_amount
    min_amount = max(3.0, 0.35 * float(unit_price))
    bedragen = [v for (v,e,u) in triples if (e or v >= min_amount) and not (u and not e)]
    if not bedragen:
        return None, None, None
    best = (None, None, None)
    # 1) Eerst met expliciete aantallen
    for q in qtys:
        expected = q * float(unit_price)
        for b in bedragen:
            err = abs(b - expected)
            rel = err / max(1.0, abs(expected))
            if err <= max_abs_err or rel <= max_rel_err:
                if best[2] is None or err < best[2]:
                    best = (round(q, 2), b, err)
    if best[0] is not None:
        return best
    # 2) Anders: infereren uit bedrag zelf
    for b in bedragen:
        q_inf = b / float(unit_price)
        if q_inf <= 0:
            continue
        err = abs(b - q_inf * float(unit_price))
        rel = err / max(1.0, abs(b))
        if err <= max_abs_err or rel <= max_rel_err:
            if best[2] is None or err < best[2]:
                best = (round(q_inf, 2), b, err)
    return best
# === Fuzzy matching helpers ===

def normalize_code(s: str) -> str:
    return re.sub(r"\D", "", str(s)).lstrip("0")

def build_prijzenboek_lookup(prijzenboek: pd.DataFrame):
    prijzenboek = prijzenboek.copy()
    prijzenboek["Taakcode_str"] = prijzenboek["Taakcode"].astype(str)
    prijzenboek["Taakcode_norm"] = prijzenboek["Taakcode_str"].apply(normalize_code)
    return prijzenboek

def fuzzy_match_code(found_code: str, prijs_codes: list, threshold: int = 92):
    if not _RAPIDFUZZ_OK or not prijs_codes:
        return (found_code if found_code in prijs_codes else None, None)
    res = process.extractOne(
        found_code,
        prijs_codes,
        scorer=fuzz.ratio,
        score_cutoff=threshold,
    )
    if res:
        best_code, score, _ = res
        return best_code, score
    return None, None

# ========== INPUT: Upload / Map / SharePoint + Auto-refresh ==========

with st.sidebar:
    st.markdown("### 📥 Bronnen kiezen")
    source = st.radio("Bron voor facturen", ["Upload", "Lokale map", "SharePoint"], index=0)

    pdf_files = []
    sharepoint_info = {}

    if source == "Upload":
        pdf_files = st.file_uploader("Facturen (PDF)", type=["pdf"], accept_multiple_files=True) or []
    elif source == "Lokale map":
        local_folder = st.text_input("Pad naar map", value=r"/Gedeelde documenten/factuurinbox/Toekomstservice")
        st.caption("De app leest alle *.pdf in deze map. Zorg dat de map lokaal bereikbaar is (of via OneDrive-sync).")
    else:
        st.caption("SharePoint map uitlezen (alleen als de python-lib 'office365-sharepoint' beschikbaar is).")
        sharepoint_info["site_url"] = st.text_input("Site URL", value="https://adjustconsulting.sharepoint.com/sites/Trevian-FinanceControl348")
        sharepoint_info["library"] = st.text_input("Document Library (bijv. 'Gedeelde documenten')", value="Gedeelde documenten")
        sharepoint_info["folder_path"] = st.text_input("Folder pad (onder library)", value="factuurinbox/Toekomstservice")
        sharepoint_info["username"] = st.text_input("Microsoft 365 e-mail", value="", placeholder="naam@bedrijf.nl")
        sharepoint_info["password"] = st.text_input("Wachtwoord", value="", type="password")

    st.markdown("### 📗 Overige input")
    xlsx_file = st.file_uploader("Prijzenboek (Excel)", type=["xlsx"])

    st.markdown("### ⚙️ Instellingen")
    TOLERANTIE = st.slider("Toegestane afwijking (€)", min_value=0.0, max_value=50.0, value=0.05, step=0.01)
    st.caption("Fuzzy matching is uitgeschakeld (alleen exacte taakcodes).")
    use_fuzzy = False
    fuzzy_threshold = 100

    st.markdown("### 🗂️ Historie & opslag")
    run_label = st.text_input("Run label (optioneel)", placeholder="bijv. Project X – juli")
    history_db_path = st.text_input("SQLite database pad", value="factuurtool_history.db")
    autosave_history = st.checkbox("Sla deze run automatisch op in historie", value=True)

    # Automatische scanopties zijn verplaatst naar het hoofdscherm (linksboven)
    # Cache (reeds verwerkte bestanden) legen
    if st.button("♻️ Reset 'reeds verwerkt' lijst"):
        try:
            con = init_db(history_db_path)  # zorgt dat de tabel bestaat
            con.execute("DELETE FROM ingested_files")
            con.commit()
            con.close()
            st.success("Lijst met reeds verwerkte bestanden is geleegd.")
        except Exception as e:
            st.error(f"Kon reset niet uitvoeren: {e}")

# Streamlit autorefresh (alleen als aangevinkt)
if enable_autorun:
    # Probeer de 'streamlit_autorefresh' module te gebruiken om de app automatisch te verversen
    try:
        from streamlit_autorefresh import st_autorefresh as _st_autorefresh
        # Interval omrekenen naar milliseconden
        _st_autorefresh(interval=int(interval_min * 60 * 1000), key="auto_refresh_key")
    except Exception:
        # Fallback: gebruikers informeren dat auto-refresh niet beschikbaar is
        st.info("Auto-refresh module niet gevonden. Installeer 'streamlit-autorefresh' of vernieuw handmatig.")

# ========== Bestanden ophalen per bron ==========

def list_local_pdfs(folder: str):
    p = Path(folder)
    if not p.exists():
        st.warning(f"Map bestaat niet: {folder}")
        return []
    return sorted([str(x) for x in p.glob("**/*.pdf")])

def list_sharepoint_pdfs(info: dict):
    if not _SP_OK:
        st.warning("SharePoint client niet beschikbaar. Installeer 'Office365-REST-Python-Client' (package: office365-sharepoint).")
        return []
    site_url = info.get("site_url")
    username = info.get("username")
    password = info.get("password")
    library = info.get("library", "Gedeelde documenten")
    folder_path = info.get("folder_path", "")

    try:
        ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))
        folder = ctx.web.get_folder_by_server_relative_url(f"/sites/{site_url.split('/sites/')[-1]}/{library}/{folder_path}")
        files = folder.files.get().execute_query()
        pdf_urls = []
        for f in files:
            if str(f.properties.get('Name', '')).lower().endswith(".pdf"):
                pdf_urls.append(f.serverRelativeUrl)
        # Download naar tijdelijke map en geef lokale paden terug
        local_paths = []
        for url in pdf_urls:
            name = url.split("/")[-1]
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
            with open(tmp.name, "wb") as fh:
                file = ctx.web.get_file_by_server_relative_url(url).download(fh).execute_query()
            local_paths.append(tmp.name)
        return local_paths
    except Exception as e:
        st.error(f"SharePoint ophalen mislukte: {e}")
        return []

def load_kasboek(df_like):
    if df_like is None:
        return None
    try:
        if hasattr(df_like, "name"):
            if str(df_like.name).lower().endswith(".csv"):
                return pd.read_csv(df_like)
            return pd.read_excel(df_like)
    except Exception:
        pass
    # indien pad
    if isinstance(df_like, str):
        if df_like.lower().endswith(".csv"):
            return pd.read_csv(df_like)
        return pd.read_excel(df_like)
    return None

# ========== Verwerken ==========

resultaten = []

def process_pdf_path(path: str, prijzenboek, prijs_codes_norm, aggregeer_per_taakcode=True, TOLERANTIE=0.05, use_fuzzy=True, fuzzy_threshold=92):
    # Default factuurnummer (fallback op bestandsnaam); wordt later overschreven
    factuurnummer = extract_factuurnummer('', os.path.basename(path))
    gebruikte_ocr = False
    regels_gevonden = []

    tmp_pdf_path = path
    # Preview (optioneel overslaan voor performance)

    # Tekst + tabellen
    try:
        with pdfplumber.open(tmp_pdf_path) as pdf:
            for page in pdf.pages:
                try:
                    tables = page.extract_tables() or []
                    for table in tables:
                        for row in table or []:
                            if row and any(row):
                                regels_gevonden.append(" ".join(str(cell) for cell in row if cell is not None))
                except Exception:
                    pass
                try:
                    txt = page.extract_text() or ""
                    regels_gevonden.extend([r for r in txt.splitlines() if r.strip()])
                except Exception:
                    pass
    except Exception:
        pass

    if not regels_gevonden:
        gebruikte_ocr = True
        regels_gevonden, ocr_codes = ocr_extract_regels_en_codes(tmp_pdf_path, poppler_path=POPLER_PATH)
    else:
        alle_teksten = "\n".join(regels_gevonden)
        raw_codes = re.findall(r"[0-9][0-9\s\-\.]{4,}[0-9]", alle_teksten)
        ocr_codes = [normalize_code(c) for c in raw_codes if 5 <= len(normalize_code(c)) <= 12]

    gevonden_codes = sorted(set(ocr_codes))

    # Bouw per-regel features zodat elke factuurregel zichtbaar wordt in een apart overzicht
    alle_regels_rows = []
    per_line_codes = []
    for idx, regel in enumerate(regels_gevonden):
        line_raw_codes = re.findall(r"[0-9][0-9\s\-\.]{4,}[0-9]", regel or "")
        line_codes = [normalize_code(c) for c in line_raw_codes if 5 <= len(normalize_code(c)) <= 12]
        per_line_codes.append(set(line_codes))
        triples = extract_bedragen_with_flags(regel)
        bedragen_vals = [v for (v, _e, _u) in triples]
        qtys = extract_qty_candidates(regel)
        alle_regels_rows.append({
            "Bestandsnaam": os.path.basename(path),
            "Factuurnummer": factuurnummer,
            "Regel_index": idx,
            "Regel": regel,
            "Codes_op_regel": ", ".join(sorted(set(line_codes))) if line_codes else None,
            "Bedragen_gevonden": ", ".join(str(b) for b in bedragen_vals) if bedragen_vals else None,
            "Qty_candidates": ", ".join(str(q) for q in qtys) if qtys else None,
            "Bevat_taakcode": bool(line_codes),
            "Verwerkingsmethode": "OCR" if gebruikte_ocr else "PDF-tabel",
        })

    # Bouw alle_teksten en bepaal factuurnummer op basis van de inhoud
    alle_teksten = "\n".join(regels_gevonden)
    factuurnummer = extract_factuurnummer(alle_teksten, os.path.basename(path))


    code_map = {}
    score_map = {}
    for fc in gevonden_codes:
        fc_norm = normalize_code(fc)
        if fc_norm in prijs_codes_norm:
            code_map[fc] = fc_norm
            score_map[fc] = 100.0
        elif use_fuzzy:
            best, score = fuzzy_match_code(fc_norm, prijs_codes_norm, threshold=fuzzy_threshold)
            if best:
                code_map[fc] = best
                score_map[fc] = float(score)

    rows = []
    for found_code, matched_code in code_map.items():
        relevante_regels = [r for r in regels_gevonden if re.sub(r"\D", "", r).find(found_code) != -1 or found_code in r]
        prijsregels = prijzenboek[prijzenboek["Taakcode_norm"] == matched_code]
        gecombineerde_prijs = prijsregels["Koopprijs (ex BTW)"].sum()

        if aggregeer_per_taakcode:
            totaal_factuur = 0.0
            aantal_geschat = 0.0
            samengevoegd_regel = []
            for regel in relevante_regels:
                q_sel, b_sel, err = choose_line_amount(regel, gecombineerde_prijs)
                if q_sel is not None and b_sel is not None:
                    try:
                        aantal_geschat += q_sel
                    except Exception:
                        pass
                    try:
                        totaal_factuur += float(b_sel)
                    except Exception:
                        pass
                else:
                    bedragen = extract_bedragen(regel)
                    q_line = pick_qty(regel, gecombineerde_prijs, bedragen, matched_code)
                    aantal_geschat += q_line
                    exp_line = (q_line or 1.0) * (gecombineerde_prijs or 0.0)
                    b_line = select_regel_bedrag(regel, bedragen, expected_total=exp_line)
                    if b_line is not None:
                        try:
                            totaal_factuur += float(b_line)
                        except Exception:
                            pass
                samengevoegd_regel.append(regel)

            verwacht = round(gecombineerde_prijs * (aantal_geschat or 1.0), 2)
            if totaal_factuur:
                afwijking_val = round(abs(totaal_factuur - verwacht), 2)
                status = "✅ Binnen marge" if afwijking_val <= TOLERANTIE else "❌ Afwijking"
            else:
                afwijking_val = None
                status = "⚠️ Bedrag niet gevonden"

            rows.append(
                {
                    "Bestandsnaam": os.path.basename(path),
                    "Factuurnummer": factuurnummer,
                    "Taakcode_gevonden": found_code,
                    "Taakcode": matched_code,
                    "Fuzzy_score": score_map.get(found_code),
                    "Aantal (geschat)": aantal_geschat,
                    "Omschrijving": ", ".join(prijsregels["Omschrijving"].astype(str).unique()),
                    "Totaalprijs boek": gecombineerde_prijs,
                    "Verwacht bedrag": verwacht,
                    "Prijs op factuur (som)": round(totaal_factuur, 2) if totaal_factuur else None,
                    "Afwijking": afwijking_val,
                    "Status": status,
                    "Regels": " | ".join(samengevoegd_regel),
                    "Verwerkingsmethode": "OCR" if gebruikte_ocr else "PDF-tabel",
                }
            )
        else:
            for regel in relevante_regels:
                q_sel, b_sel, err = choose_line_amount(regel, gecombineerde_prijs)
                if q_sel is not None and b_sel is not None:
                    aantal_geschat = q_sel
                    regel_som = b_sel
                else:
                    bedragen = extract_bedragen(regel)
                    aantal_geschat = pick_qty(regel, gecombineerde_prijs, bedragen, matched_code)
                    expected_line = (aantal_geschat or 1.0) * (gecombineerde_prijs or 0.0)
                    regel_som = select_regel_bedrag(regel, bedragen, expected_total=expected_line) or 0.0
                verwacht = round(gecombineerde_prijs * (aantal_geschat or 1.0), 2)
                afwijking_val = round(abs(regel_som - verwacht), 2) if regel_som else None
                status = ("✅ Binnen marge" if afwijking_val is not None and afwijking_val <= TOLERANTIE
                          else ("❌ Afwijking" if regel_som else "⚠️ Bedrag niet gevonden"))
                rows.append(
                    {
                        "Bestandsnaam": os.path.basename(path),
                    "Factuurnummer": factuurnummer,
                        "Taakcode_gevonden": found_code,
                        "Taakcode": matched_code,
                        "Fuzzy_score": score_map.get(found_code),
                        "Aantal (geschat)": aantal_geschat,
                        "Omschrijving": ", ".join(prijsregels["Omschrijving"].astype(str).unique()),
                        "Totaalprijs boek": gecombineerde_prijs,
                        "Verwacht bedrag": verwacht,
                        "Prijs op factuur (som)": round(regel_som, 2) if regel_som else None,
                        "Afwijking": afwijking_val,
                        "Status": status,
                        "Regels": regel,
                        "Verwerkingsmethode": "OCR" if gebruikte_ocr else "PDF-tabel",
                    }
                )
    # === Verwerk eventuele codes die niet zijn gematcht in het prijzenboek ===
    unmatched_codes = [fc for fc in gevonden_codes if fc not in code_map]
    if unmatched_codes:
        # definieer trefwoorden waarop we regels met niet-relevante informatie willen filteren
        skip_keywords = [
            # Administratieve of betalingsgerelateerde termen die duiden op non-productregels
            "iban", "banknummer", "rabo", "rabobank", "rekening", "overmaken", "restant",
            "datum", "factuurnummer", "factuurnr", "werkadres", "werkorder", "opdrachtnr", "opdracht", "uw nummer",
            "bij betaling", "betalingskenmerk", "betaal", "betaaldatum", "uiterste",
            "g-rekening", "loonkosten", "loonkostenbestanddeel", "loon",
            # Totale en btw regels
            "totaal", "subtotaal", "btw verlegd",
        ]
        for uc in unmatched_codes:
            relevante_regels = [r for r in regels_gevonden if re.sub(r"\D", "", r).find(uc) != -1 or uc in r]
            for regel in relevante_regels:
                # filter regels met niet-relevante sleutelwoorden
                if not regel:
                    continue
                low = regel.lower()
                if any(kw in low for kw in skip_keywords):
                    continue
                bedragen = extract_bedragen(regel)
                qty_candidates = extract_qty_candidates(regel)
                try:
                    aantal_unknown = float(qty_candidates[0]) if qty_candidates else 1.0
                except Exception:
                    aantal_unknown = 1.0
                prijs_op_regel = select_regel_bedrag(regel, bedragen)
                try:
                    prijs_val = float(prijs_op_regel) if prijs_op_regel is not None else None
                except Exception:
                    prijs_val = None
                if prijs_val is None and not qty_candidates:
                    continue
                rows.append({
                    "Bestandsnaam": os.path.basename(path),
                    "Factuurnummer": factuurnummer,
                    "Taakcode_gevonden": uc,
                    "Taakcode": None,
                    "Fuzzy_score": None,
                    "Aantal (geschat)": aantal_unknown,
                    "Omschrijving": None,
                    "Totaalprijs boek": None,
                    "Verwacht bedrag": None,
                    "Prijs op factuur (som)": prijs_val,
                    "Afwijking": None,
                    "Status": "⚠️ Onbekende taakcode",
                    "Regels": regel,
                    "Verwerkingsmethode": "OCR" if gebruikte_ocr else "PDF-tabel",
                })
    # === Voeg regels ZONDER taakcode toe ===
    # Selecteer regels waar geen code op de regel is gedetecteerd
    lines_with_any_code = {i for i, cs in enumerate(per_line_codes) if cs}
    for idx, regel in enumerate(regels_gevonden):
        if idx in lines_with_any_code:
            continue
        if not regel:
            continue
        bedragen = extract_bedragen(regel)
        qty_candidates = extract_qty_candidates(regel)
        prijs_op_regel = select_regel_bedrag(regel, bedragen)
        try:
            prijs_val = float(prijs_op_regel) if prijs_op_regel is not None else None
        except Exception:
            prijs_val = None
        # Alleen toevoegen als er iets nuttigs staat (bedrag of kwantiteit)
        if prijs_val is None and not qty_candidates:
            continue
        try:
            aantal_unknown = float(qty_candidates[0]) if qty_candidates else 1.0
        except Exception:
            aantal_unknown = 1.0
        rows.append({
            "Bestandsnaam": os.path.basename(path),
            "Factuurnummer": factuurnummer,
            "Taakcode_gevonden": None,
            "Taakcode": None,
            "Fuzzy_score": None,
            "Aantal (geschat)": aantal_unknown,
            "Omschrijving": None,
            "Totaalprijs boek": None,
            "Verwacht bedrag": None,
            "Prijs op factuur (som)": prijs_val,
            "Afwijking": None,
            "Status": "⚠️ Geen taakcode",
            "Regels": regel,
            "Verwerkingsmethode": "OCR" if gebruikte_ocr else "PDF-tabel",
        })

    return rows, alle_regels_rows

# ========== Hoofdlogica: bron ophalen en verwerken ==========

def get_pdf_paths_from_source(source, pdf_files, local_folder, sharepoint_info):
    paths = []
    if source == "Upload":
        # Schrijf geüploade bestanden tijdelijk weg met behoud van de oorspronkelijke bestandsnaam.
        # Lees de upload één keer in geheugen om lege writes door herhaald f.read() te voorkomen.
        for f in (pdf_files or []):
            try:
                original_name = os.path.basename(getattr(f, "name", "upload.pdf"))
            except Exception:
                original_name = "upload.pdf"
            try:
                data = f.getvalue() if hasattr(f, "getvalue") else f.read()
            except Exception:
                # laatste redmiddel
                try:
                    f.seek(0)
                    data = f.read()
                except Exception:
                    data = b""
            tmp_dir = tempfile.gettempdir()
            tmp_path = os.path.join(tmp_dir, original_name)
            try:
                with open(tmp_path, "wb") as out:
                    out.write(data)
            except Exception:
                with tempfile.NamedTemporaryFile(delete=False, suffix="_" + original_name) as tmp:
                    tmp.write(data)
                    tmp_path = tmp.name
            paths.append(tmp_path)
    elif source == "Lokale map":
        if local_folder:
            paths = list_local_pdfs(local_folder)
    else:
        paths = list_sharepoint_pdfs(sharepoint_info)
    return paths

# Trigger scannen: bij upload is er input; bij map/SharePoint doen we scan_now of auto
should_scan = False
if source == "Upload":
    should_scan = bool(pdf_files and xlsx_file)
else:
    should_scan = (scan_now or enable_autorun) and xlsx_file

if should_scan:
    prijzenboek = pd.read_excel(xlsx_file)
    prijzenboek = build_prijzenboek_lookup(prijzenboek)
    prijs_codes_norm = prijzenboek["Taakcode_norm"].tolist()

    # Kasboek wordt niet meer gebruikt

    # Bestanden ophalen
    local_folder = locals().get("local_folder", None)
    paths = get_pdf_paths_from_source(source, pdf_files, local_folder, sharepoint_info)

    total = len(paths)
    progress = st.progress(0, text="Start met verwerken…")
    all_rows = []
    all_line_rows = []

    for idx, path in enumerate(paths):
        try:
            if source != "Upload":
                # double-processing voorkomen
                try:
                    mtime = os.path.getmtime(path)
                except Exception:
                    mtime = float(datetime.now().timestamp())
                if is_already_ingested(history_db_path, path, mtime):
                    progress.progress(int(((idx + 1) / max(1, total)) * 100), text=f"Overgeslagen (reeds verwerkt): {os.path.basename(path)}")
                    continue

            rows, per_line_rows = process_pdf_path(
                path,
                prijzenboek,
                prijs_codes_norm,
                aggregeer_per_taakcode=True,
                TOLERANTIE=TOLERANTIE,
                use_fuzzy=use_fuzzy,
                fuzzy_threshold=fuzzy_threshold
            )
            all_rows.extend(rows)
            all_line_rows.extend(per_line_rows)

            if source != "Upload":
                try:
                    mark_ingested(history_db_path, path, os.path.getmtime(path))
                except Exception:
                    pass

            progress.progress(int(((idx + 1) / max(1, total)) * 100), text=f"Verwerkt: {os.path.basename(path)}")
        except Exception as e:
            st.warning(f"Fout bij verwerken van {os.path.basename(path)}: {e}")

    if all_rows or all_line_rows:
        resultaat_df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=[
            "Bestandsnaam","Factuurnummer","Taakcode_gevonden","Taakcode","Fuzzy_score","Aantal (geschat)",
            "Omschrijving","Totaalprijs boek","Verwacht bedrag","Prijs op factuur (som)","Afwijking","Status","Regels","Verwerkingsmethode"
        ])
        alle_regels_df = pd.DataFrame(all_line_rows) if all_line_rows else pd.DataFrame(columns=[
            "Bestandsnaam","Factuurnummer","Regel_index","Regel","Codes_op_regel","Bedragen_gevonden","Qty_candidates","Bevat_taakcode","Verwerkingsmethode"
        ])

                # Normaliseer numerieke kolommen naar float voor Pandas/Excel
        for _col in ["Totaalprijs boek","Verwacht bedrag","Prijs op factuur (som)","Afwijking","Aantal (geschat)","Fuzzy_score"]:
            if _col in resultaat_df.columns:
                resultaat_df[_col] = pd.to_numeric(resultaat_df[_col], errors="coerce")

        
        # Factuuroverzicht aanmaken: totaal per factuur en afwijking ten opzichte van verwacht
        factuur_summary = resultaat_df.groupby("Bestandsnaam").agg({
            "Prijs op factuur (som)": "sum",
            "Verwacht bedrag": "sum",
        }).reset_index().rename(columns={
            "Prijs op factuur (som)": "Totaal prijs op factuur",
            "Verwacht bedrag": "Totaal verwacht bedrag",
        })
        factuur_summary["Totaal afwijking"] = (pd.to_numeric(factuur_summary["Totaal prijs op factuur"], errors="coerce") - pd.to_numeric(factuur_summary["Totaal verwacht bedrag"], errors="coerce")).abs().round(2)
        factuur_summary["Status factuur"] = factuur_summary["Totaal afwijking"].apply(
            lambda diff: "✅ Binnen marge" if pd.notna(diff) and diff <= TOLERANTIE else "❌ Afwijking"
        )
        st.markdown("## 📊 Resultaten")
        # Maak tabs voor overzicht, afwijkingen en factuuroverzicht en export
        tabs = st.tabs(["📄 Alle regels", "✅ Binnen marge", "❌ Afwijkingen & Overig", "🧾 Factuur overzicht", "📥 Export"])

        # Binnen marge
        with tabs[0]:
            st.data_editor(
                alle_regels_df,
                num_rows="dynamic",
                use_container_width=True,
                key="alle_regels",
            )
        # Binnen marge
        with tabs[1]:
            st.data_editor(
                resultaat_df[resultaat_df["Status"] == "✅ Binnen marge"],
                num_rows="dynamic",
                use_container_width=True,
                key="binnen_marge",
            )
        # Afwijkingen
        with tabs[2]:
            st.data_editor(
                resultaat_df[resultaat_df["Status"] != "✅ Binnen marge"],
                num_rows="dynamic",
                use_container_width=True,
                key="afwijkingen",
            )
        # Factuuroverzicht
        with tabs[3]:
            st.data_editor(
                factuur_summary,
                num_rows="dynamic",
                use_container_width=True,
                key="factuur_overzicht",
            )
        # Export tab
        with tabs[4]:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                resultaat_df.to_excel(writer, index=False, sheet_name="Resultaten")
                # voeg factuuroverzicht toe als aparte sheet
                factuur_summary.to_excel(writer, index=False, sheet_name="Factuur overzicht")
                # voeg alle regels sheet toe
                alle_regels_df.to_excel(writer, index=False, sheet_name="Alle regels")
            st.download_button(
                label="📥 Download resultaten als Excel",
                data=buffer.getvalue(),
                file_name="factuurcontrole_resultaten.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        if autosave_history:
            try:
                run_id = save_run_and_results(history_db_path, run_label, resultaat_df)
                st.success(f"🗂️ Run opgeslagen in historie (run_id={run_id}).")
            except Exception as e:
                st.warning(f"Kon run niet opslaan in historie: {e}")

        st.markdown("---")
        st.markdown(f"*Laatste run: {pd.Timestamp.now():%d-%m-%Y %H:%M}*")

# === DASHBOARD HISTORIE ===

st.markdown("## 📈 Historie")
colA, colB = st.columns([2, 1])
with colA:
    st.caption("Overzicht van eerdere runs op basis van de SQLite-historie.")
with colB:
    refresh = st.button("🔄 Vernieuw")

try:
    con = init_db(history_db_path)
    runs_df = pd.read_sql_query("SELECT id, ts, COALESCE(label, '') AS label FROM runs ORDER BY id DESC", con)
    results_df = pd.read_sql_query(
        """
        SELECT r.id AS run_id, r.ts, r.label,
               COUNT(res.id) AS regels,
               SUM(CASE WHEN res.status = '❌ Afwijking' THEN 1 ELSE 0 END) AS afwijkingen,
               SUM(CASE WHEN res.status = '✅ Binnen marge' THEN 1 ELSE 0 END) AS binnen_marge
        FROM runs r
        LEFT JOIN results res ON res.run_id = r.id
        GROUP BY r.id, r.ts, r.label
        ORDER BY r.id DESC
        """,
        con,
    )
    con.close()

    if not runs_df.empty:
        st.dataframe(results_df, use_container_width=True)

        k1, k2, k3 = st.columns(3)
        totaal_runs = len(results_df)
        totaal_regels = int(results_df["regels"].fillna(0).sum())
        totaal_afwijkingen = int(results_df["afwijkingen"].fillna(0).sum())
        pct_afwijking = (totaal_afwijkingen / totaal_regels * 100) if totaal_regels else 0
        k1.metric("Aantal runs", totaal_runs)
        k2.metric("Totaal regels", totaal_regels)
        k3.metric("% Afwijking", f"{pct_afwijking:.1f}%")
    # Grafieken verwijderd in deze 'lichte' versie
    # Top-taakcodes grafiek verwijderd in deze 'lichte' versie
    else:
        st.info("Nog geen historie gevonden. Voer een run uit en zet opslag aan.")
except Exception as e:
    st.warning(f"Kon historie niet laden: {e}")
