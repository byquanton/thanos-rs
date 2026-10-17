# thanos-rs

Ein kleines Tool in Rust, um Minecraft-Weltordner zu verkleinern, indem ungenutzte oder leere Chunk-Daten entfernt werden. Nützlich, wenn deine Welt über die Zeit unnötig groß geworden ist.

## Was macht das Tool

thanos-rs schaut sich deine Weltdaten an und entfernt Bereiche, die nicht (mehr) gebraucht werden. Das Ergebnis ist ein kleinerer Ordner, ohne dass Minecraft Probleme beim Laden bekommt.

**Wichtig:** Mach immer vorher ein Backup deiner Welt. Auch wenn das Tool getestet ist, sollte man bei allem, was Weltdateien verändert, auf Nummer sicher gehen.

## Installation

Es gibt zwei Wege, das Tool zu benutzen.

### Weg 1 — Fertige Datei nutzen (einfacher)

1. Gehe zum Reiter [Releases](../../releases)
2. Lade dir unter "Assets" die passende `.exe` herunter
3. Fertig, kein Rust oder Cargo nötig

### Weg 2 — Selbst bauen

Voraussetzung: Rust ist installiert ([rustup.rs](https://rustup.rs))
git clone https://github.com/Markimark09/thanos-rs.git
cd thanos-rs
cargo build --release

Nach dem Bauen liegt die fertige Datei hier:
target/release/thanos-rs.exe

Der Build dauert je nach PC ungefähr 5 bis 15 Minuten. Das ist normal.

## Benutzung

Öffne eine Kommandozeile (PowerShell oder Terminal) im Ordner, in dem `thanos-rs.exe` liegt.

Hilfe anzeigen:
.\thanos-rs.exe --help

Dort werden dir alle verfügbaren Optionen angezeigt.

## Bevor du es an deiner echten Welt benutzt

- Kopiere deine Weltordner an einen anderen Ort (Backup)
- Teste zuerst an der Kopie
- Prüfe danach, ob Minecraft die bearbeitete Welt noch normal öffnet
- Erst wenn alles passt, an der echten Welt anwenden (optional, auf eigenes Risiko)

## Häufige Probleme

**Es gibt keinen `target`-Ordner im Repository**
Das ist Absicht. Der `target`-Ordner entsteht erst lokal beim Bauen mit `cargo build`. Er wird nicht mit hochgeladen, weil er nur Baudateien enthält, die sich jederzeit neu erzeugen lassen.

**Welche Datei ist die richtige nach dem Bauen?**
Immer die aus `target/release/`, nicht aus `target/debug/`. Der Release-Build ist optimiert und deutlich schneller.

**Build dauert sehr lange oder hängt**
Normal beim ersten Mal, da alle Abhängigkeiten heruntergeladen und kompiliert werden müssen. Rechner währenddessen nicht ausschalten.

## Mitmachen

Wer Fehler findet oder Verbesserungsvorschläge hat, kann gerne ein Issue eröffnen oder einen Pull Request erstellen.

## Getestete Minecraft-Versionen

| Version | Funktioniert |
|---|---|
| 26.1 | ✅ |
| 26.2 | ✅ (sollte funktionieren, da gleiches Speicherformat) |
| 26.3 | ✅ (sollte funktionieren, da gleiches Speicherformat) |
| 26.1.2 | ✅ |

Das Tool arbeitet direkt auf den Region-Dateien (.mca) der Welt, unabhängig von der genauen Minecraft-Version. Solange sich das Speicherformat nicht ändert, sollte es auch mit zukünftigen Versionen ab 26.1 funktionieren. Getestet wurde konkret mit Version 26.1.

Vor dem Einsatz an einer wichtigen Welt trotzdem immer zuerst an einer Kopie testen.

Sources:
- https://wiki.vg/NBT
- https://wiki.vg/Region_Files
