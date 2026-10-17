use flate2::read::{GzDecoder, ZlibDecoder};
use rayon::prelude::*;
use std::fs;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

const SECTOR_SIZE: u64 = 4096;

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
// Minimaler NBT-Leser: sucht "InhabitedTime" (Long).
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
        Some(i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }
    fn name(&mut self) -> Option<&'a [u8]> {
        let n = self.u16()? as usize;
        self.take(n)
    }
}

/// Ueberspringt den Payload eines NBT-Tags.
fn skip_payload(r: &mut Reader, tag: u8, depth: u32) -> Option<()> {
    if depth > 512 {
        return None;
    }
    match tag {
        1 => { r.take(1)?; }
        2 => { r.take(2)?; }
        3 | 5 => { r.take(4)?; }
        4 | 6 => { r.take(8)?; }
        7 => {
            let n = r.i32()?;
            if n < 0 { return None; }
            r.take(n as usize)?;
        }
        8 => {
            let n = r.u16()? as usize;
            r.take(n)?;
        }
        9 => {
            let t = r.u8()?;
            let n = r.i32()?;
            if n < 0 { return None; }
            for _ in 0..n {
                skip_payload(r, t, depth + 1)?;
            }
        }
        10 => loop {
            let t = r.u8()?;
            if t == 0 { break; }
            r.name()?;
            skip_payload(r, t, depth + 1)?;
        },
        11 => {
            let n = r.i32()?;
            if n < 0 { return None; }
            r.take((n as usize).checked_mul(4)?)?;
        }
        12 => {
            let n = r.i32()?;
            if n < 0 { return None; }
            r.take((n as usize).checked_mul(8)?)?;
        }
        _ => return None,
    }
    Some(())
}

/// Liest InhabitedTime aus dem NBT-Root eines Chunks.
fn read_inhabited_time(raw: &[u8]) -> Option<i64> {
    let mut r = Reader { d: raw, p: 0 };
    if r.u8()? != 10 {
        return None;
    }
    r.name()?;
    loop {
        let t = r.u8()?;
        if t == 0 {
            return None;
        }
        let name = r.name()?;
        if t == 4 && name == b"InhabitedTime" {
            return r.i64();
        }
        skip_payload(&mut r, t, 1)?;
    }
}

// ---------------------------------------------------------------
// Dateien kopieren (komplette Welt, Region wird danach ersetzt)
// ---------------------------------------------------------------
fn copy_all(input: &Path, output: &Path, p: &Progress) -> std::io::Result<()> {
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

/// Region-Ordner finden: "region" im Welt-Root und
/// "dimensions/<namespace>/<name>/region" (Vanilla-Layout).
fn find_region_dirs(root: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();

    let direct = root.join("region");
    if direct.is_dir() {
        result.push(direct);
    }

    let dims = root.join("dimensions");
    if let Ok(ns_iter) = fs::read_dir(&dims) {
        for ns in ns_iter.flatten() {
            if let Ok(dim_iter) = fs::read_dir(ns.path()) {
                for dim in dim_iter.flatten() {
                    let r = dim.path().join("region");
                    if r.is_dir() {
                        result.push(r);
                    }
                }
            }
        }
    }
    result
}

struct KeptChunk {
    index: usize,
    timestamp: [u8; 4],
    compression: u8,
    data: Vec<u8>,
}

/// Verarbeitet eine einzelne .mca-Datei.
fn process_region(src: &Path, dst: &Path, threshold: i64, p: &Progress) -> Result<(), String> {
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
            continue;
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

        // Externe Chunks (.mcc) bleiben unveraendert und werden nur gemeldet.
        if compression & 0x80 != 0 {
            p.log(format!("{name}: Chunk {index} ist extern (.mcc), bleibt erhalten"));
            continue;
        }
        if !(1..=3).contains(&compression) || chunk_size <= 1 {
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

        // Im Zweifel behalten.
        let keep = match if ok { read_inhabited_time(&raw) } else { None } {
            Some(t) => t > threshold,
            None => true,
        };

        if keep {
            let mut ts = [0u8; 4];
            let t0 = SECTOR_SIZE as usize + o;
            ts.copy_from_slice(&header[t0..t0 + 4]);
            kept.push(KeptChunk { index, timestamp: ts, compression, data });
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
        let t0 = SECTOR_SIZE as usize + o;
        out_header[t0..t0 + 4].copy_from_slice(&c.timestamp);

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

    p.bytes_after.fetch_add(2 * SECTOR_SIZE + body.len() as u64, Ordering::Relaxed);
    Ok(())
}

/// Gibt den tatsaechlich verwendeten Ausgabe-Pfad zurueck (None = Fehler).
pub fn run(input: PathBuf, output: PathBuf, threshold: i64, threads: usize, p: &Progress) -> Option<PathBuf> {
    let result = match run_inner(&input, &output, threshold, threads, p) {
        Ok(final_out) => Some(final_out),
        Err(e) => {
            if let Ok(mut err) = p.error.lock() {
                *err = Some(e);
            }
            None
        }
    };
    p.finished.store(true, Ordering::SeqCst);
    result
}

fn run_inner(input: &Path, output: &Path, threshold: i64, threads: usize, p: &Progress) -> Result<PathBuf, String> {
    if !input.is_dir() {
        return Err("Der Eingabe-Ordner existiert nicht.".into());
    }
    if !input.join("level.dat").exists() {
        return Err("In diesem Ordner liegt keine level.dat. Das ist keine Minecraft-Welt.".into());
    }

    // Freien Ausgabe-Namen waehlen: Name, Name_2, Name_3 ...
    let base = output.to_path_buf();
    let mut output_path = base.clone();
    let mut n = 2;
    while output_path.exists() {
        let name = base.file_name().unwrap_or_default().to_string_lossy().to_string();
        output_path = base.with_file_name(format!("{name}_{n}"));
        n += 1;
    }
    let output = output_path.as_path();
    p.log(format!("Ausgabe: {}", output.display()));

    if let (Ok(a), Some(parent)) = (input.canonicalize(), output.parent()) {
        if let Ok(pa) = parent.canonicalize() {
            if pa.starts_with(&a) {
                return Err("Der Ausgabe-Ordner darf nicht innerhalb der Welt liegen.".into());
            }
        }
    }

    fs::create_dir_all(output).map_err(|e| format!("Ausgabe-Ordner nicht erstellbar: {e}"))?;

    p.log("Kopiere Weltdateien ...");
    copy_all(input, output, p).map_err(|e| format!("Kopieren fehlgeschlagen: {e}"))?;
    if p.cancel.load(Ordering::Relaxed) {
        return Err("Abgebrochen.".into());
    }

    let mut jobs: Vec<(PathBuf, PathBuf)> = Vec::new();
    for rd in find_region_dirs(input) {
        let rel = rd.strip_prefix(input).unwrap();
        let out_dir = output.join(rel);
        fs::create_dir_all(&out_dir).map_err(|e| format!("Region-Ordner nicht erstellbar: {e}"))?;
        if let Ok(entries) = fs::read_dir(&rd) {
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
        return Err(format!("{} Datei(en) mit Fehlern. Erster: {}", errs.len(), errs[0]));
    }
    p.log("Fertig.");
    Ok(output.to_path_buf())
}

pub fn zip_dir(dir: &Path, p: &Progress) -> Result<(), String> {
    use zip::write::SimpleFileOptions;

    p.log("Packe ZIP ...");
    let name = dir.file_name().and_then(|n| n.to_str()).unwrap_or("world");
    let zip_path = dir.with_file_name(format!("{name}.zip"));
    let file = fs::File::create(&zip_path).map_err(|e| format!("ZIP: {e}"))?;
    let mut zip = zip::ZipWriter::new(file);
    let opts = SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);

    let base = dir.parent().unwrap_or(dir);
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in fs::read_dir(&d).map_err(|e| e.to_string())? {
            let path = entry.map_err(|e| e.to_string())?.path();
            let rel = path.strip_prefix(base).unwrap().to_string_lossy().replace('\\', "/");
            if path.is_dir() {
                zip.add_directory(format!("{rel}/"), opts).map_err(|e| e.to_string())?;
                stack.push(path);
            } else {
                zip.start_file(rel, opts).map_err(|e| e.to_string())?;
                let data = fs::read(&path).map_err(|e| e.to_string())?;
                zip.write_all(&data).map_err(|e| e.to_string())?;
            }
        }
    }
    zip.finish().map_err(|e| e.to_string())?;
    p.log(format!("ZIP fertig: {}", zip_path.display()));
    Ok(())
}