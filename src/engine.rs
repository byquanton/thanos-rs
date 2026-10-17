use flate2::read::{GzDecoder, ZlibDecoder};
use rayon::prelude::*;
use std::fs;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

const SECTOR_SIZE: u64 = 4096;

/// Gemeinsamer Zustand zwischen Worker-Thread und GUI.
pub struct Progress {
    pub total_files: AtomicUsize,
    pub done_files: AtomicUsize,
    pub bytes_before: AtomicU64,
    pub bytes_after: AtomicU64,
    pub chunks_kept: AtomicU64,
    pub chunks_removed: AtomicU64,
    pub finished: AtomicBool,
    pub cancel: AtomicBool,
    pub log: Mutex<Vec<String>>,
    pub error: Mutex<Option<String>>,
}

impl Progress {
    pub fn new() -> Self {
        Progress {
            total_files: AtomicUsize::new(0),
            done_files: AtomicUsize::new(0),
            bytes_before: AtomicU64::new(0),
            bytes_after: AtomicU64::new(0),
            chunks_kept: AtomicU64::new(0),
            chunks_removed: AtomicU64::new(0),
            finished: AtomicBool::new(false),
            cancel: AtomicBool::new(false),
            log: Mutex::new(Vec::new()),
            error: Mutex::new(None),
        }
    }

    pub fn log(&self, msg: impl Into<String>) {
        if let Ok(mut l) = self.log.lock() {
            l.push(msg.into());
            let len = l.len();
            if len > 500 {
                l.drain(0..len - 500);
            }
        }
    }
}

// ---------------------------------------------------------------
// Minimaler NBT-Leser: sucht einen Long-Tag "InhabitedTime".
// Gibt None zurueck, wenn nichts gefunden wurde oder die Daten
// kaputt sind (nie Panic).
// ---------------------------------------------------------------
struct Reader<'a> {
    d: &'a [u8],
    p: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.p.checked_add(n)?;
        if end > self.d.len() {
            return None;
        }
        let s = &self.d[self.p..end];
        self.p = end;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u16(&mut self) -> Option<u16> {
        let b = self.take(2)?;
        Some(u16::from_be_bytes([b[0], b[1]]))
    }
    fn i32(&mut self) -> Option<i32> {
        let b = self.take(4)?;
        Some(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn i64(&mut self) -> Option<i64> {
        let b = self.take(8)?;
        Some(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
    fn name(&mut self) -> Option<&'a [u8]> {
        let n = self.u16()? as usize;
        self.take(n)
    }
}

/// Ueberspringt den Inhalt eines Tags (ohne Typ-Byte und Namen).
fn skip_payload(r: &mut Reader, tag: u8, depth: u32) -> Option<()> {
    if depth > 512 {
        return None;
    }
    match tag {
        1 => {
            r.take(1)?;
        }
        2 => {
            r.take(2)?;
        }
        3 | 5 => {
            r.take(4)?;
        }
        4 | 6 => {
            r.take(8)?;
        }
        7 => {
            let n = r.i32()?;
            if n < 0 {
                return None;
            }
            r.take(n as usize)?;
        }
        8 => {
            let n = r.u16()? as usize;
            r.take(n)?;
        }
        9 => {
            let t = r.u8()?;
            let n = r.i32()?;
            if n < 0 {
                return None;
            }
            for _ in 0..n {
                skip_payload(r, t, depth + 1)?;
            }
        }
        10 => loop {
            let t = r.u8()?;
            if t == 0 {
                break;
            }
            r.name()?;
            skip_payload(r, t, depth + 1)?;
        },
        11 => {
            let n = r.i32()?;
            if n < 0 {
                return None;
            }
            r.take((n as usize).checked_mul(4)?)?;
        }
        12 => {
            let n = r.i32()?;
            if n < 0 {
                return None;
            }
            r.take((n as usize).checked_mul(8)?)?;
        }
        _ => return None,
    }
    Some(())
}

/// Liest ein Compound. Sucht "InhabitedTime" direkt darin, sonst
/// (eine Ebene tiefer) in "Level".
fn find_in_compound(r: &mut Reader, allow_level: bool, depth: u32) -> Option<i64> {
    let mut found: Option<i64> = None;
    loop {
        let t = r.u8()?;
        if t == 0 {
            return found;
        }
        let name = r.name()?;
        if t == 4 && (name == b"InhabitedTime" || name == b"inhabitedTime") {
            found = Some(r.i64()?);
        } else if t == 10 && allow_level && name == b"Level" {
            let inner = find_in_compound(r, false, depth + 1)?;
            if found.is_none() {
                found = Some(inner);
            }
        } else {
            skip_payload(r, t, depth + 1)?;
        }
    }
}

pub fn read_inhabited_time(nbt: &[u8]) -> Option<i64> {
    let mut r = Reader { d: nbt, p: 0 };
    if r.u8()? != 10 {
        return None;
    }
    r.name()?; // Name des Wurzel-Tags (meist leer)
    find_in_compound(&mut r, true, 0)
}

// ---------------------------------------------------------------
// Dateien kopieren (alles ausser Region-Ordner)
// ---------------------------------------------------------------
fn copy_except_region(input: &Path, output: &Path, p: &Progress) -> std::io::Result<()> {
    let mut stack = vec![input.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            if p.cancel.load(Ordering::Relaxed) {
                return Ok(());
            }
            let path = entry?.path();
            let rel = path.strip_prefix(input).unwrap();
            let dest = output.join(rel);
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name == "region" {
                    continue; // wird separat verarbeitet
                }
                fs::create_dir_all(&dest)?;
                stack.push(path);
            } else {
                if let Some(parent) = dest.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::copy(&path, &dest)?;
            }
        }
    }
    Ok(())
}

/// Alle Ordner mit Region-Dateien finden (Overworld, DIM-1, DIM1, ...)
fn find_region_dirs(root: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        if let Ok(rd) = fs::read_dir(&dir) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    if path.file_name().and_then(|n| n.to_str()) == Some("region") {
                        result.push(path);
                    } else {
                        stack.push(path);
                    }
                }
            }
        }
    }
    result
}

struct KeptChunk {
    index: usize, // Position 0..1023 in der Tabelle
    timestamp: [u8; 4],
    compression: u8,
    data: Vec<u8>,
}

/// Verarbeitet eine einzelne .mca-Datei.
fn process_region(
    src: &Path,
    dst: &Path,
    threshold: i64,
    p: &Progress,
) -> Result<(), String> {
    let name = src.file_name().and_then(|n| n.to_str()).unwrap_or("?").to_string();
    let mut file = fs::File::open(src).map_err(|e| format!("{name}: {e}"))?;
    let file_len = file.metadata().map_err(|e| format!("{name}: {e}"))?.len();
    p.bytes_before.fetch_add(file_len, Ordering::Relaxed);

    if file_len < 2 * SECTOR_SIZE {
        p.log(format!("Uebersprungen (leer/zu klein): {name}"));
        return Ok(());
    }

    let mut header = vec![0u8; (2 * SECTOR_SIZE) as usize];
    file.read_exact(&mut header).map_err(|e| format!("{name}: {e}"))?;

    let mut kept: Vec<KeptChunk> = Vec::new();

    for index in 0..1024usize {
        if p.cancel.load(Ordering::Relaxed) {
            return Ok(());
        }
        let o = index * 4;
        let loc = u32::from_be_bytes([header[o], header[o + 1], header[o + 2], header[o + 3]]);
        let sector_offset = (loc >> 8) as u64;
        let num_sectors = (loc & 0xFF) as u64;
        if sector_offset == 0 || num_sectors == 0 {
            continue; // nicht generiert
        }
        let pos = sector_offset * SECTOR_SIZE;
        if pos + 5 > file_len {
            p.log(format!("{name}: Chunk {index} ausserhalb der Datei, uebersprungen"));
            continue;
        }
        if file.seek(SeekFrom::Start(pos)).is_err() {
            continue;
        }
        let mut len_buf = [0u8; 4];
        if file.read_exact(&mut len_buf).is_err() {
            continue;
        }
        let chunk_size = i32::from_be_bytes(len_buf);
        let mut comp = [0u8; 1];
        if file.read_exact(&mut comp).is_err() {
            continue;
        }
        let compression = comp[0];

        // Externe Chunks (Bit 128 gesetzt) liegen in .mcc-Dateien: sicherheitshalber behalten wir sie nicht,
        // da wir sie nicht mitkopieren. Wir melden es nur.
        if compression & 0x80 != 0 {
            p.log(format!("{name}: Chunk {index} ist extern (.mcc), uebersprungen"));
            continue;
        }
        if !(1..=3).contains(&compression) {
            p.log(format!("{name}: Chunk {index} unbekannte Kompression {compression}"));
            continue;
        }
        if chunk_size <= 1 {
            continue;
        }
        let payload_len = (chunk_size as usize) - 1;
        if pos + 5 + payload_len as u64 > file_len {
            p.log(format!("{name}: Chunk {index} abgeschnitten, uebersprungen"));
            continue;
        }
        let mut data = vec![0u8; payload_len];
        if file.read_exact(&mut data).is_err() {
            continue;
        }

        let mut raw: Vec<u8> = Vec::new();
        let ok = match compression {
            1 => GzDecoder::new(Cursor::new(&data)).read_to_end(&mut raw).is_ok(),
            2 => ZlibDecoder::new(Cursor::new(&data)).read_to_end(&mut raw).is_ok(),
            _ => {
                raw = data.clone();
                true
            }
        };

        // Bei Lesefehler behalten wir den Chunk lieber (sicherer als loeschen).
        let inhabited = if ok { read_inhabited_time(&raw) } else { None };
        let keep = match inhabited {
            Some(t) => t > threshold,
            None => true,
        };

        if keep {
            let mut ts = [0u8; 4];
            ts.copy_from_slice(&header[(SECTOR_SIZE as usize + o)..(SECTOR_SIZE as usize + o + 4)]);
            kept.push(KeptChunk {
                index,
                timestamp: ts,
                compression,
                data,
            });
            p.chunks_kept.fetch_add(1, Ordering::Relaxed);
        } else {
            p.chunks_removed.fetch_add(1, Ordering::Relaxed);
        }
    }

    if kept.is_empty() {
        p.log(format!("Region {name} ist nach der Bereinigung leer und wird weggelassen"));
        return Ok(());
    }

    let mut out_header = vec![0u8; (2 * SECTOR_SIZE) as usize];
    let mut body: Vec<u8> = Vec::new();
    let mut next_sector: u64 = 2;

    for c in &kept {
        let total = 4 + 1 + c.data.len() as u64;
        let sectors = (total + SECTOR_SIZE - 1) / SECTOR_SIZE;
        if sectors > 255 {
            return Err(format!("{name}: Chunk {} ist zu gross", c.index));
        }
        let loc: u32 = ((next_sector as u32) << 8) | sectors as u32;
        let o = c.index * 4;
        out_header[o..o + 4].copy_from_slice(&loc.to_be_bytes());
        out_header[SECTOR_SIZE as usize + o..SECTOR_SIZE as usize + o + 4]
            .copy_from_slice(&c.timestamp);

        body.extend_from_slice(&(c.data.len() as i32 + 1).to_be_bytes());
        body.push(c.compression);
        body.extend_from_slice(&c.data);
        let pad = (sectors * SECTOR_SIZE - total) as usize;
        body.extend(std::iter::repeat(0u8).take(pad));
        next_sector += sectors;
    }

    let mut out = fs::File::create(dst).map_err(|e| format!("{name}: {e}"))?;
    out.write_all(&out_header).map_err(|e| format!("{name}: {e}"))?;
    out.write_all(&body).map_err(|e| format!("{name}: {e}"))?;
    out.flush().map_err(|e| format!("{name}: {e}"))?;

    let out_len = 2 * SECTOR_SIZE + body.len() as u64;
    p.bytes_after.fetch_add(out_len, Ordering::Relaxed);
    Ok(())
}

/// Hauptfunktion: laeuft im Worker-Thread.
pub fn run(input: PathBuf, output: PathBuf, threshold: i64, threads: usize, p: &Progress) {
    let result = run_inner(&input, &output, threshold, threads, p);
    if let Err(e) = result {
        if let Ok(mut err) = p.error.lock() {
            *err = Some(e);
        }
    }
    p.finished.store(true, Ordering::SeqCst);
}

fn run_inner(
    input: &Path,
    output: &Path,
    threshold: i64,
    threads: usize,
    p: &Progress,
) -> Result<(), String> {
    if !input.is_dir() {
        return Err("Der Eingabe-Ordner existiert nicht.".into());
    }
    if !input.join("level.dat").exists() {
        return Err("In diesem Ordner liegt keine level.dat. Das ist keine Minecraft-Welt.".into());
    }
    let same = match (input.canonicalize(), output.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    };
    if same {
        return Err("Eingabe und Ausgabe duerfen nicht derselbe Ordner sein.".into());
    }
    if output.exists() {
        return Err("Der Ausgabe-Ordner existiert schon. Bitte einen neuen Namen waehlen.".into());
    }
    if let Ok(out_abs) = output.canonicalize().or_else(|_| {
        output
            .parent()
            .map(|pp| pp.canonicalize().map(|c| c.join(output.file_name().unwrap_or_default())))
            .unwrap_or_else(|| Ok(output.to_path_buf()))
    }) {
        if let Ok(in_abs) = input.canonicalize() {
            if out_abs.starts_with(&in_abs) {
                return Err("Der Ausgabe-Ordner darf nicht innerhalb der Welt liegen.".into());
            }
        }
    }

    fs::create_dir_all(output).map_err(|e| format!("Ausgabe-Ordner nicht erstellbar: {e}"))?;

    p.log("Kopiere Weltdateien ...");
    copy_except_region(input, output, p).map_err(|e| format!("Kopieren fehlgeschlagen: {e}"))?;
    if p.cancel.load(Ordering::Relaxed) {
        return Err("Abgebrochen.".into());
    }

    // Alle Region-Ordner sammeln und Dateiliste bauen
    let region_dirs = find_region_dirs(input);
    let mut jobs: Vec<(PathBuf, PathBuf)> = Vec::new();
    for rd in &region_dirs {
        let rel = rd.strip_prefix(input).unwrap();
        let out_dir = output.join(rel);
        fs::create_dir_all(&out_dir).map_err(|e| format!("Region-Ordner nicht erstellbar: {e}"))?;
        if let Ok(entries) = fs::read_dir(rd) {
            for e in entries.flatten() {
                let path = e.path();
                if path.extension().and_then(|x| x.to_str()) == Some("mca") {
                    let dst = out_dir.join(path.file_name().unwrap());
                    jobs.push((path, dst));
                }
            }
        }
    }
    p.total_files.store(jobs.len(), Ordering::Relaxed);
    p.log(format!("{} Region-Dateien gefunden.", jobs.len()));

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .build()
        .map_err(|e| format!("Thread-Pool: {e}"))?;

    let errors: Mutex<Vec<String>> = Mutex::new(Vec::new());
    pool.install(|| {
        jobs.par_iter().for_each(|(src, dst)| {
            if p.cancel.load(Ordering::Relaxed) {
                return;
            }
            if let Err(e) = process_region(src, dst, threshold, p) {
                if let Ok(mut v) = errors.lock() {
                    v.push(e);
                }
            }
            p.done_files.fetch_add(1, Ordering::Relaxed);
        });
    });

    if p.cancel.load(Ordering::Relaxed) {
        return Err("Abgebrochen. Die Ausgabe ist unvollstaendig, bitte loeschen.".into());
    }
    let errs = errors.into_inner().unwrap_or_default();
    if !errs.is_empty() {
        return Err(format!(
            "{} Datei(en) mit Fehlern. Erster Fehler: {}",
            errs.len(),
            errs[0]
        ));
    }
    p.log("Fertig.");
    Ok(())
}