def load_and_scale_bibliography(
    xml_path: str,
    base_venues: list[str],
    extra_venues: list[str],
    scale_factor: int = 2
):
    import xml.etree.ElementTree as ET

    if scale_factor < 1 or (scale_factor & (scale_factor - 1)) != 0:
        raise ValueError("scale_factor muss 1, 2, 4, 8, ... sein.")

    # --- XML laden ---
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Hilfsfunktion: Venue-Feld robust auslesen
    def get_venue(entry):
        # je nach XML-Struktur ggf. anpassen:
        # häufig: journal, booktitle, venue
        for tag in ("journal", "booktitle", "venue"):
            node = entry.find(tag)
            if node is not None and node.text:
                return node.text.strip()
        return None

    # Hilfsfunktion: Eintrag in dict umwandeln (bei Bedarf Felder erweitern)
    def parse_entry(entry):
        data = {}
        for child in entry:
            if child.text:
                data[child.tag] = child.text.strip()
        return data

    # Alle bibliographischen Einträge (Tag-Liste bei Bedarf erweitern)
    all_entries = [
        e for e in root
        if e.tag in {"article", "inproceedings", "proceedings", "book", "incollection", "phdthesis", "mastersthesis"}
    ]

    base_venue_set = set(base_venues)
    extra_venue_set = set(extra_venues)

    # 1) Basisdatenbestand (entspricht "tatsächlicher Größe der Datei" im bisherigen Setup)
    base_entries = [e for e in all_entries if get_venue(e) in base_venue_set]
    base_size = len(base_entries)

    if base_size == 0:
        raise ValueError("Basisdatenbestand ist 0. Prüfe base_venues bzw. XML-Struktur.")

    target_size = base_size * scale_factor

    # 2) Kandidaten aus zusätzlichen Venues
    extra_entries = [e for e in all_entries if get_venue(e) in extra_venue_set and e not in base_entries]

    # 3) Ergebnis zusammensetzen
    result_entries = list(base_entries)

    # Falls nicht genug zusätzliche Daten vorhanden sind, zyklisch auffüllen
    if len(result_entries) < target_size:
        if not extra_entries:
            raise ValueError(
                "Keine zusätzlichen Venues gefunden. Bitte extra_venues erweitern oder Venue-Mapping prüfen."
            )

        i = 0
        while len(result_entries) < target_size:
            result_entries.append(extra_entries[i % len(extra_entries)])
            i += 1

    # Falls zu groß, abschneiden
    result_entries = result_entries[:target_size]

    # 4) In gewünschtes Format parsen
    parsed = [parse_entry(e) for e in result_entries]
    return parsed