import gzip
import xml.etree.ElementTree as ET
from pathlib import Path
from edge_model import EdgeModelBuilder, PUBLICATION_TYPES, _venue_from_key, _resolve_named_entities

def parse_with_phase1_logic(zip_path: Path, output_path: Path):
    print(f"[PARSER] Öffne {zip_path.name} und extrahiere Daten...")
    
    augsten_counts = {"vldb": 0, "sigmod": 0, "icde": 0}
    total_filtered_publications = 0
    
    # Target-Keys
    target_keys = ["SchmittKAMM23", "HutterAK0L22", "SchalerHS23"]
    key_intervals = {key: {"start": None, "end": None} for key in target_keys}
    
    parser = ET.XMLPullParser(["end"])
    
    current_line_num = 1

    with gzip.open(zip_path, "rt", encoding="utf-8") as f_in, open(output_path, "w", encoding="utf-8") as f_out:
        f_out.write('<?xml version="1.0" encoding="utf-8"?>\n')
        current_line_num += 1
        f_out.write('<bib>\n')
        current_line_num += 1
        
        for line in f_in:
            clean_line = _resolve_named_entities(line)
            parser.feed(clean_line)
            
            for event, elem in parser.read_events():
                if elem.tag in PUBLICATION_TYPES:
                    key = elem.attrib.get("key", "")
                    venue = _venue_from_key(key)
                    
                    if venue:
                        total_filtered_publications += 1
                        
                        # Count Nikolaus Augsten
                        authors = [author.text for author in elem.findall("author") if author.text]
                        if "Nikolaus Augsten" in authors:
                            augsten_counts[venue] += 1
                        
                        xml_str = ET.tostring(elem, encoding="utf-8").decode("utf-8")
                        pub_lines = [f"  {l}" for l in xml_str.strip().splitlines()]
                        pub_line_count = len(pub_lines)
                        block_start = current_line_num
                        block_end = current_line_num + pub_line_count - 1
                        
                        # Check Keys in diesem Publikationsblock existiert
                        for t_key in target_keys:
                            if t_key in key:
                                key_intervals[t_key]["start"] = block_start
                                key_intervals[t_key]["end"] = block_end
                        
                        for pub_line in pub_lines:
                            f_out.write(pub_line + "\n")
                            current_line_num += 1
                            
                    # Speicherbereinigung
                    elem.clear()

        f_out.write('</bib>\n')

    file_size_kb = output_path.stat().st_size / 1024.0

    print("Anzahl Publikationen von 'Nikolaus Augsten':")
    print(f"  - ICDE: {augsten_counts['icde']}")
    print(f"  - VLDB: {augsten_counts['vldb']}")
    print(f"  - SIGMOD: {augsten_counts['sigmod']}")
    print("Position (Zeilennummer von, bis) der Publikationen:")
    for key in target_keys:
        start = key_intervals[key]["start"]
        end = key_intervals[key]["end"]
        if start and end:
            print(f"  - {key}: Lines {start} to {end}")
        else:
            print(f"  - {key}: Nicht im gefilterten Datensatz enthalten")
    print(f"Größe der Datei my_small_bib.xml in kB: {file_size_kb:.2f} kB")
    print(f"Zeilenanzahl gesamt:                    {current_line_num} Zeilen")

if __name__ == "__main__":
    base_dir = Path(__file__).parent
    dblp_zip = base_dir / "dblp.xml.gz"
    output_xml = base_dir / "my_small_bib.xml"
    
    if not dblp_zip.exists():
        print(f"Fehler: {dblp_zip.name} not found.")
    else:
        parse_with_phase1_logic(dblp_zip, output_xml)